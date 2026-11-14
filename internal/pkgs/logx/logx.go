package logx

import (
	"encoding/json"
	"log"
)

const gap = " "

// Fatal logs values and terminates the process.
func Fatal(v ...any) {
	xs := append([]any{"❌"}, v...)
	log.Fatal(xs...)
}

// Fatalf logs a formatted message and terminates the process.
func Fatalf(format string, v ...any) {
	log.Fatalf("❌ "+format, v...)
}

// Error logs values at error level.
func Error(v ...any) {
	xs := append([]any{"❌"}, v...)
	log.Print(xs...)
}

// Errorf logs a formatted message at error level.
func Errorf(format string, v ...any) {
	log.Printf("❌ "+format, v...)
}

// Warn logs values at warning level.
func Warn(v ...any) { //width: A not W
	xs := append([]any{"⚠️", gap}, v...)
	log.Print(xs...)
}

// Warnf logs a formatted message at warning level.
func Warnf(format string, v ...any) {
	log.Printf("⚠️ "+gap+format, v...)
}

// Pin logs values at informational level.
func Pin(v ...any) {
	xs := append([]any{"📌"}, v...)
	log.Println(xs...)
}

// Pinf logs a formatted message at informational level.
func Pinf(format string, v ...any) {
	log.Printf("📌 "+format, v...)
}

// PinJSON logs a JSON representation at informational level.
func PinJSON(v ...any) {
	xs := []any{"📌"}
	for _, x := range v {
		str, ok := x.(string)
		if ok {
			xs = append(xs, str)
			continue
		}
		j, _ := json.MarshalIndent(x, "", "  ")
		xs = append(xs, string(j))
	}
	log.Println(xs...)
}

// Debug logs values at debug level.
func Debug(v ...any) {
	xs := append([]any{"🐛"}, v...)
	log.Println(xs...)
}

// Debugf logs a formatted message at debug level.
func Debugf(format string, v ...any) {
	log.Printf("🐛 "+format, v...)
}

// DebugJSON logs a JSON representation at debug level.
func DebugJSON(v ...any) {
	xs := []any{"🐛"}
	for _, x := range v {
		str, ok := x.(string)
		if ok {
			xs = append(xs, str)
			continue
		}
		j, _ := json.MarshalIndent(x, "", "  ")
		xs = append(xs, string(j))
	}
	log.Println(xs...)
}
