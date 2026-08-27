// Command server is the GitHub Actions Monitor: it wires together config,
// the GitHub API clients, the SQLite store, the inbound webhook receiver,
// the background poller, and the dashboard/API HTTP server.
package main

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vlussenburg-org/ghes-actions-monitor/internal/api"
	"github.com/vlussenburg-org/ghes-actions-monitor/internal/config"
	"github.com/vlussenburg-org/ghes-actions-monitor/internal/ghclient"
	"github.com/vlussenburg-org/ghes-actions-monitor/internal/poller"
	"github.com/vlussenburg-org/ghes-actions-monitor/internal/store"
	"github.com/vlussenburg-org/ghes-actions-monitor/internal/webhook"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger.Info("starting github actions monitor",
		"org", cfg.Org, "is_ghec", cfg.IsGHEC, "port", cfg.Port, "db_path", cfg.DBPath)

	clients, err := ghclient.New(cfg)
	if err != nil {
		return err
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			logger.Error("failed to close store", "error", cerr)
		}
	}()

	// Prefer the least-privilege App client for polling if configured;
	// otherwise fall back to the admin PAT.
	pollClient := clients.App
	if pollClient == nil {
		pollClient = clients.Admin
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var p *poller.Poller
	if pollClient != nil {
		p = &poller.Poller{
			Client:                 &poller.GHClientAdapter{Client: pollClient},
			Store:                  st,
			Org:                    cfg.Org,
			Logger:                 logger,
			WorkflowInterval:       cfg.WorkflowPollInterval,
			RunnerInterval:         cfg.RunnerPollInterval,
			SpotCheckWindow:        cfg.WorkflowSpotCheckWindow,
			StaleRunReconcileAfter: cfg.StaleRunReconcileAfter,
		}
		if clients.RateLimit != nil {
			p.RateLimiter = clients.RateLimit
		}
		go p.Run(ctx)
	} else {
		logger.Warn("no GitHub client configured; poller disabled")
	}

	githubBaseURL := "https://github.com"
	if !cfg.IsGHEC {
		githubBaseURL = cfg.GHESBaseURL
	}
	actionClient := clients.Admin
	if actionClient == nil {
		actionClient = pollClient
	}
	apiServer := &api.Server{
		Store:         st,
		Org:           cfg.Org,
		GitHubBaseURL: githubBaseURL,
		RunController: &poller.GHClientAdapter{Client: actionClient},
		RateLimit:     clients.RateLimit,
	}
	if p != nil {
		apiServer.Refresher = p
	}
	mux := apiServer.Routes()
	mux.Handle("/", noCache(http.FileServer(http.Dir("web/static"))))

	webhookHandler := &webhook.Handler{Secret: cfg.WebhookSecret, Store: st, Logger: logger}

	var handler http.Handler = mux
	if cfg.RequiresAuth() {
		handler = basicAuth(mux, cfg.AuthUsername, cfg.AuthToken)
		logger.Info("HTTP Basic Auth enabled for dashboard and API")
	} else {
		logger.Warn("no AUTH_USERNAME/AUTH_TOKEN configured; dashboard and API are unauthenticated and must be placed behind an authenticating proxy")
	}

	topMux := http.NewServeMux()
	topMux.Handle("/webhook/github", webhookHandler)
	topMux.Handle("/", handler)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           accessLog(topMux, logger),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-serverErr:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		next.ServeHTTP(w, r)
	})
}

// basicAuth requires HTTP Basic Auth credentials matching the configured
// username/token before delegating to next, using constant-time comparisons
// to avoid leaking credential length/content via timing.
func basicAuth(next http.Handler, username, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		userMatch := subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1
		passMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(token)) == 1
		if !ok || !userMatch || !passMatch {
			w.Header().Set("WWW-Authenticate", `Basic realm="GitHub Actions Monitor", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func accessLog(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &accessLogResponseWriter{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", time.Since(started).Seconds()*1000,
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	})
}

type accessLogResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *accessLogResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *accessLogResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}
