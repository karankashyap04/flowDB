package driver

// The driver will communicate between the Go code and the actual database

import (
	Logger "flowDB/pkg/logger"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

type Driver struct {
	myMutex sync.Mutex
	allMutexes map[string]*sync.Mutex
	dbDir string
	log Logger.Logger
}

func CreateDB(dbDir string, loggerOptions *Logger.LoggerOptions) (*Driver, error) {
	var options Logger.LoggerOptions
	if loggerOptions == nil {
		options.Logger = lumber.NewConsoleLogger(lumber.INFO)
	} else {
		options = *loggerOptions // want the value; not the pointer -- pointer dereference
	}

	cleanedDir := filepath.Clean(dbDir)
	var dbDriver Driver = Driver{
		allMutexes: make(map[string]*sync.Mutex),
		dbDir: cleanedDir,
		log: options.Logger,
	}

	if doesDBExist(cleanedDir) {
		options.Logger.Debug("Error: A database already exists at directory %s\n", cleanedDir)
		return &dbDriver, nil
	}

	options.Logger.Debug("Creating a database at directory %s\n", cleanedDir)
	err := os.Mkdir(cleanedDir, 0755)
	return &dbDriver, err

}

func doesDBExist(cleanedDir string) bool {
	_, err := os.Stat(cleanedDir)
	return !os.IsNotExist(err)
}