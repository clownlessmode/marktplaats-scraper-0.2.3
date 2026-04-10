package marktplaats

import "strings"

// FormatText mirrors Python format_text: collapse whitespace.
func FormatText(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
