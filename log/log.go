// Package log provides structured logging using Go 1.21+ log/slog.
// It maintains backward compatibility with the original Printf-style API
// while adding structured logging capabilities.
package log

import (
	"context"
	"fmt"
	stdlog "log"
	"log/slog"
	"os"
	"sync/atomic"
)

// Level represents a logging level
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	ErrorLevel
)

// currentLevel is the minimum level for logging (atomic for thread safety)
var currentLevel atomic.Int32

func init() {
	currentLevel.Store(int32(InfoLevel))
}

// Logger interface for logging (backward compatibility)
type Logger interface {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

// slogLevel converts our Level to slog.Level
func slogLevel(l Level) slog.Level {
	switch l {
	case DebugLevel:
		return slog.LevelDebug
	case InfoLevel:
		return slog.LevelInfo
	case ErrorLevel:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// logger wraps slog.Logger with backward-compatible Printf interface
type logger struct {
	level  Level
	prefix string
	slog   *slog.Logger
}

// Global loggers
var (
	Debug = newLogger(DebugLevel, "")
	Info  = newLogger(InfoLevel, "")
	Error = newLogger(ErrorLevel, "")
	Fatal = newLogger(ErrorLevel, "") // Fatal uses error level but calls os.Exit
)

// defaultHandler creates the default slog handler
func defaultHandler() slog.Handler {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug, // Allow all levels, we filter ourselves
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Customize time format if needed
			return a
		},
	}
	return slog.NewTextHandler(os.Stderr, opts)
}

func newLogger(level Level, prefix string) *logger {
	return &logger{
		level:  level,
		prefix: prefix,
		slog:   slog.New(defaultHandler()),
	}
}

// New creates a new logger with the given level and prefix
func New(level Level, prefix string) *logger {
	return newLogger(level, prefix)
}

// SetPrefix sets the prefix for all global loggers
func SetPrefix(prefix string) {
	Debug.prefix = prefix
	Info.prefix = prefix
	Error.prefix = prefix
	Fatal.prefix = prefix
}

// SetLevel sets the minimum logging level
func SetLevel(level string) {
	switch level {
	case "debug":
		currentLevel.Store(int32(DebugLevel))
	case "info":
		currentLevel.Store(int32(InfoLevel))
	case "error":
		currentLevel.Store(int32(ErrorLevel))
	}
}

// GetLevel returns the current logging level
func GetLevel() Level {
	return Level(currentLevel.Load())
}

// NewStdLogger creates a standard library logger for compatibility with
// external libraries (like sarama) that require *log.Logger
func NewStdLogger(l Logger) *stdlog.Logger {
	return stdlog.New(os.Stderr, "", stdlog.LstdFlags)
}

// shouldLog returns true if the logger's level should be logged
func (l *logger) shouldLog() bool {
	return l.level >= Level(currentLevel.Load())
}

// Printf logs a formatted message
func (l *logger) Printf(format string, v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	msg := fmt.Sprintf(format, v...)
	if l.prefix != "" {
		msg = l.prefix + msg
	}
	l.log(msg)
}

// Print logs a message
func (l *logger) Print(v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	msg := fmt.Sprint(v...)
	if l.prefix != "" {
		msg = l.prefix + msg
	}
	l.log(msg)
}

// Println logs a message with newline
func (l *logger) Println(v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	msg := fmt.Sprintln(v...)
	if l.prefix != "" {
		msg = l.prefix + msg
	}
	l.log(msg)
}

// Fatal logs a message and exits
func (l *logger) Fatal(v ...interface{}) {
	msg := fmt.Sprint(v...)
	if l.prefix != "" {
		msg = l.prefix + msg
	}
	l.slog.Log(context.Background(), slog.LevelError, msg)
	os.Exit(1)
}

// Fatalf logs a formatted message and exits
func (l *logger) Fatalf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if l.prefix != "" {
		msg = l.prefix + msg
	}
	l.slog.Log(context.Background(), slog.LevelError, msg)
	os.Exit(1)
}

// log is the internal logging method
func (l *logger) log(msg string) {
	l.slog.Log(context.Background(), slogLevel(l.level), msg)
}

// Structured logging methods (new API)

// Debug logs a debug message with structured attributes
func (l *logger) Debug(msg string, args ...any) {
	if l.level < Level(currentLevel.Load()) {
		return
	}
	l.slog.Debug(l.prefix+msg, args...)
}

// Info logs an info message with structured attributes
func (l *logger) Info(msg string, args ...any) {
	if l.level < Level(currentLevel.Load()) {
		return
	}
	l.slog.Info(l.prefix+msg, args...)
}

// Error logs an error message with structured attributes
func (l *logger) Error(msg string, args ...any) {
	l.slog.Error(l.prefix+msg, args...)
}

// With returns a new logger with the given attributes
func (l *logger) With(args ...any) *logger {
	return &logger{
		level:  l.level,
		prefix: l.prefix,
		slog:   l.slog.With(args...),
	}
}

// WithGroup returns a new logger with the given group name
func (l *logger) WithGroup(name string) *logger {
	return &logger{
		level:  l.level,
		prefix: l.prefix,
		slog:   l.slog.WithGroup(name),
	}
}

// Slog returns the underlying slog.Logger for advanced use cases
func (l *logger) Slog() *slog.Logger {
	return l.slog
}

var _ Logger = (*logger)(nil)
