package logger

import "github.com/jcelliot/lumber"

type Logger interface {
	Trace(string, ...interface{})
	Debug(string, ...interface{})
	Info(string, interface{})
	Warn(string, interface{})
	Error(string, interface{})
	Fatal(string, interface{})
}