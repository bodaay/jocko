package log

import (
	stdlog "log"
	"os"
)

// Level represents a logging level
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	ErrorLevel
)

var currentLevel = InfoLevel

// Logger interface for logging
type Logger interface {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

var (
	Debug = &logger{level: DebugLevel, l: stdlog.New(os.Stderr, "[DEBUG] ", stdlog.LstdFlags|stdlog.Lshortfile)}
	Info  = &logger{level: InfoLevel, l: stdlog.New(os.Stderr, "[INFO] ", stdlog.LstdFlags|stdlog.Lshortfile)}
	Error = &logger{level: ErrorLevel, l: stdlog.New(os.Stderr, "[ERROR] ", stdlog.LstdFlags|stdlog.Lshortfile)}
	Fatal = &logger{level: ErrorLevel, l: stdlog.New(os.Stderr, "[FATAL] ", stdlog.LstdFlags|stdlog.Lshortfile)}
)

type logger struct {
	prefix string
	level  Level
	l      *stdlog.Logger
}

func New(level Level, prefix string) *logger {
	l := &logger{prefix: prefix, level: level}
	switch level {
	case DebugLevel:
		l.l = stdlog.New(os.Stderr, "[DEBUG] "+prefix, stdlog.LstdFlags|stdlog.Lshortfile)
	case InfoLevel:
		l.l = stdlog.New(os.Stderr, "[INFO] "+prefix, stdlog.LstdFlags|stdlog.Lshortfile)
	case ErrorLevel:
		l.l = stdlog.New(os.Stderr, "[ERROR] "+prefix, stdlog.LstdFlags|stdlog.Lshortfile)
	}
	return l
}

func SetPrefix(prefix string) {
	for _, logger := range []*logger{Debug, Info, Error} {
		logger.prefix = prefix
	}
}

func SetLevel(level string) {
	switch level {
	case "debug":
		currentLevel = DebugLevel
	case "info":
		currentLevel = InfoLevel
	case "error":
		currentLevel = ErrorLevel
	}
}

func NewStdLogger(l Logger) *stdlog.Logger {
	return stdlog.New(os.Stderr, "", stdlog.LstdFlags)
}

func (l *logger) shouldLog() bool {
	return l.level >= currentLevel
}

func (l *logger) Printf(format string, v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	if l.prefix == "" {
		l.l.Printf(format, v...)
	} else {
		l.l.Printf(l.prefix+format, v...)
	}
}

func (l *logger) Print(v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	if l.prefix == "" {
		l.l.Print(v...)
	} else {
		l.l.Print(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Println(v ...interface{}) {
	if !l.shouldLog() {
		return
	}
	if l.prefix == "" {
		l.l.Println(v...)
	} else {
		l.l.Println(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Fatal(v ...interface{}) {
	if l.prefix == "" {
		l.l.Fatal(v...)
	} else {
		l.l.Fatal(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	if l.prefix == "" {
		l.l.Fatalf(format, v...)
	} else {
		l.l.Fatalf(l.prefix+format, v...)
	}
}

var _ Logger = (*logger)(nil)
