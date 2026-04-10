package adminbot

import (
	"reflect"
	"testing"
)

func TestParseEmailLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		line, wantE, wantP string
		ok                 bool
	}{
		{"", "", "", false},
		{"  ", "", "", false},
		{"# comment", "", "", false},
		{"user@gmail.com:secret", "user@gmail.com", "secret", true},
		{"USER@Gmail.COM;apppass", "user@gmail.com", "apppass", true},
		{"x@y.com\tpwd", "x@y.com", "pwd", true},
		{"only@gmail.com", "only@gmail.com", "", true},
		{"nocolon@x.com", "nocolon@x.com", "", true},
		{"badline", "", "", false},
	}
	for _, tc := range tests {
		e, p, ok := ParseEmailLine(tc.line)
		if ok != tc.ok || e != tc.wantE || p != tc.wantP {
			t.Errorf("ParseEmailLine(%q) = (%q,%q,%v) want (%q,%q,%v)", tc.line, e, p, ok, tc.wantE, tc.wantP, tc.ok)
		}
	}
}

func TestParseEmailsText(t *testing.T) {
	t.Parallel()
	got := ParseEmailsText("a@b.c:1\n\n#x\n  \nc@d.e;2")
	want := [][2]string{{"a@b.c", "1"}, {"c@d.e", "2"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseEmailsText = %#v want %#v", got, want)
	}
}

func TestParseEmailsCSV(t *testing.T) {
	t.Parallel()
	t.Run("standard", func(t *testing.T) {
		csv := "email,apppassword\nfoo@gmail.com,pass1\nbar@gmail.com,pass2\n"
		got := ParseEmailsCSV(csv)
		want := [][2]string{{"foo@gmail.com", "pass1"}, {"bar@gmail.com", "pass2"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v want %#v", got, want)
		}
	})
	t.Run("russian_headers", func(t *testing.T) {
		csv := "Почта,Пароль\nx@y.com,secret\n"
		got := ParseEmailsCSV(csv)
		if len(got) != 1 || got[0][0] != "x@y.com" || got[0][1] != "secret" {
			t.Fatalf("got %#v", got)
		}
	})
	t.Run("semicolon_csv", func(t *testing.T) {
		csv := "email;password\na@b.c;p1\n"
		got := ParseEmailsCSV(csv)
		if len(got) != 1 || got[0][0] != "a@b.c" {
			t.Fatalf("got %#v", got)
		}
	})
	t.Run("empty", func(t *testing.T) {
		if ParseEmailsCSV("") != nil {
			t.Fatal("expected nil")
		}
	})
	t.Run("no_email_column", func(t *testing.T) {
		if ParseEmailsCSV("a,b\n1,2\n") != nil {
			t.Fatal("expected nil")
		}
	})
}

func TestParsePageAndSafeEmail(t *testing.T) {
	t.Parallel()
	p, s := parsePageAndSafeEmail("2_foo_bar")
	if p != 2 || s != "foo_bar" {
		t.Fatalf("got page=%d safe=%q", p, s)
	}
	p, s = parsePageAndSafeEmail("noscore")
	if p != 0 || s != "noscore" {
		t.Fatalf("got page=%d safe=%q", p, s)
	}
}

func TestMax(t *testing.T) {
	t.Parallel()
	if max(1, 2) != 2 || max(3, 2) != 3 {
		t.Fatal(max(1, 2), max(3, 2))
	}
}
