package adminbot

import (
	"path/filepath"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	var empty Config
	if err := empty.validate(); err == nil {
		t.Fatal("empty config should fail")
	}
	c := Config{AdminBotToken: "x", AdminChatID: 1, DBPath: filepath.Join(t.TempDir(), "x.db")}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigLoadFromEnv(t *testing.T) {
	t.Setenv("ADMIN_BOT_TOKEN", "envtok")
	t.Setenv("ADMIN_CHAT_ID", "42")
	t.Setenv("CLIENT_BOT_TOKEN", "client")
	var c Config
	c.LoadFromEnv()
	if c.AdminBotToken != "envtok" || c.AdminChatID != 42 || c.ClientBotToken != "client" {
		t.Fatalf("%+v", c)
	}
}

func TestConfigBOTTokenFallback(t *testing.T) {
	t.Setenv("ADMIN_BOT_TOKEN", "a")
	t.Setenv("CLIENT_BOT_TOKEN", "")
	t.Setenv("BOT_TOKEN", "fallback")
	var c Config
	c.LoadFromEnv()
	if c.ClientBotToken != "fallback" {
		t.Fatalf("got %q", c.ClientBotToken)
	}
}
