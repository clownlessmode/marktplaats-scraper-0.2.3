package proxy

import (
	"fmt"
	"os"
	"path/filepath"
)

// LoadProxiesSearch ищет файл: path, затем scraper-golang/proxies.txt если имя по умолчанию proxies.txt.
func LoadProxiesSearch(name string) ([]Entry, string, error) {
	isDefault := name == "proxies.txt"
	try := []string{name}
	if isDefault {
		try = append(try, filepath.Join("scraper-golang", "proxies.txt"))
	}
	for _, p := range try {
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			continue
		}
		entries, err := LoadFile(p)
		if err != nil {
			return nil, p, err
		}
		return entries, p, nil
	}
	if !isDefault {
		return nil, "", fmt.Errorf("файл прокси не найден: %q", name)
	}
	return nil, "", nil
}
