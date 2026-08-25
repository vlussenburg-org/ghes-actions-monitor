// Command server is the GitHub Actions Monitor: it wires together config,
// the GitHub API clients, the SQLite store, the inbound webhook receiver,
// the background poller, and the dashboard/API HTTP server.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vlussenburg/ghes-actions-monitor/internal/api"
	"github.com/vlussenburg/ghes-actions-monitor/internal/config"
	"github.com/vlussenburg/ghes-actions-monitor/internal/ghclient"
	"github.com/vlussenburg/ghes-actions-monitor/internal/poller"
	"github.com/vlussenburg/ghes-actions-monitor/internal/store"
	"github.com/vlussenburg/ghes-actions-monitor/internal/webhook"
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

	if pollClient != nil {
		p := &poller.Poller{
			Client:           &poller.GHClientAdapter{Client: pollClient},
			Store:            st,
			Org:              cfg.Org,
			Logger:           logger,
			WorkflowInterval: cfg.WorkflowPollInterval,
			RunnerInterval:   cfg.RunnerPollInterval,
			HistoryInterval:  cfg.HistoryPollInterval,
		}
		go p.Run(ctx)
	} else {
		logger.Warn("no GitHub client configured; poller disabled")
	}

	apiServer := &api.Server{Store: st, Org: cfg.Org}
	mux := apiServer.Routes()
	mux.Handle("/", http.FileServer(http.Dir("web/static")))

	webhookHandler := &webhook.Handler{Secret: cfg.WebhookSecret, Store: st, Logger: logger}
	mux.Handle("/webhook/github", webhookHandler)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
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
