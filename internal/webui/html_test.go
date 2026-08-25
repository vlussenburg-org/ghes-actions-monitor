package webui

import (
	"os"
	"regexp"
	"testing"

	"github.com/dop251/goja/parser"
)

func TestDashboardInlineScriptsParse(t *testing.T) {
	html, err := os.ReadFile("../../web/static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}

	scripts := regexp.MustCompile(`(?is)<script[^>]*>(.*?)</script>`).FindAllSubmatch(html, -1)
	if len(scripts) == 0 {
		t.Fatal("expected at least one inline script in dashboard HTML")
	}

	for i, script := range scripts {
		if _, err := parser.ParseFile(nil, "web/static/index.html", string(script[1]), 0); err != nil {
			t.Fatalf("inline script %d has invalid JavaScript: %v", i+1, err)
		}
	}
}
