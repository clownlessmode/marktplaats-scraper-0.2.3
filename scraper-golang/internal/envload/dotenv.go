// Package envload подгружает переменные из .env как в scraper-python (не перезаписывает уже заданные в ОС).
package envload

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// LoadDefaults ищет .env в текущей директории, затем scraper-golang/.env и scraper-python/.env
// (удобно при запуске из корня репозитория). Позже в списке подставляются только отсутствующие ключи.
func LoadDefaults() {
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}
	paths := []string{
		filepath.Join(wd, ".env"),
		filepath.Join(wd, "scraper-golang", ".env"),
		filepath.Join(wd, "scraper-python", ".env"),
		// запуск из каталога scraper-golang — общий .env у соседнего scraper-python
		filepath.Join(wd, "..", "scraper-python", ".env"),
	}
	seen := make(map[string]struct{})
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		_ = loadFile(abs)
	}
}

func loadFile(path string) error {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		i := strings.IndexByte(line, '=')
		if i <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:i])
		val := strings.TrimSpace(line[i+1:])
		val = unquote(val)
		if key == "" {
			continue
		}
		if os.Getenv(key) != "" {
			continue
		}
		_ = os.Setenv(key, val)
	}
	return sc.Err()
}

func unquote(s string) string {
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			return strings.Trim(s, `"`)
		}
		if s[0] == '\'' && s[len(s)-1] == '\'' {
			return strings.Trim(s, `'`)
		}
	}
	return s
}
