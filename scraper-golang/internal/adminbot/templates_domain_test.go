package adminbot

import (
	"strings"
	"testing"
)

func TestFormatTemplateExampleBody(t *testing.T) {
	t.Parallel()
	out := FormatTemplateExampleBody(templateExampleBody())
	if !strings.Contains(out, "iPhone 14 Pro") || !strings.Contains(out, "899") {
		t.Fatalf("unexpected output: %s", out)
	}
	if strings.Contains(out, "{title}") {
		t.Fatal("placeholders should be replaced")
	}
}

func TestTplAddStep1HTML(t *testing.T) {
	t.Parallel()
	h := tplAddStep1HTML()
	if !strings.Contains(h, "{url}") || !strings.Contains(h, "Новый шаблон") {
		t.Fatal("missing expected fragments")
	}
}

func TestTemplateVarDescriptionsKeys(t *testing.T) {
	t.Parallel()
	for k := range TemplateVarDescriptions {
		if k == "" {
			t.Fatal("empty key")
		}
	}
}
