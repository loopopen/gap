package dashboard

import (
	"regexp"
	"strings"
)

func PathValue(pattern string, path string) string {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	re := regexp.MustCompile(`\{[^}]+\}`)
	pattern = re.ReplaceAllString(pattern, "(\\d+)/")
	re = regexp.MustCompile(pattern)
	ms := re.FindStringSubmatch(path)
	if len(ms) <= 1 {
		return ""
	}
	return ms[1]
}
