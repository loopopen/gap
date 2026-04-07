package dashboard

import (
	"regexp"
	"strings"
)

func GinPath(path string) string {
	if strings.HasSuffix(path, "*") {
		return path + "all"
	}

	re := regexp.MustCompile(`\{([^}]+)\}`)
	path = re.ReplaceAllString(path, ":$1")
	return path
}
