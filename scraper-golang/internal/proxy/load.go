package proxy

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadFile reads proxies.txt-style file: one URI per line, # comments, empty lines skipped.
func LoadFile(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []Entry
	s := bufio.NewScanner(f)
	lineNo := 0
	for s.Scan() {
		lineNo++
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		e, err := ParseLine(line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
		out = append(out, e)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
