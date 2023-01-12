package driver

// The driver will communicate between the Go code and the actual database

import (
	"encoding/json"
	Logger "flowDB/pkg/logger"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jcelliott/lumber"
)

type Driver struct {
	myMutex sync.Mutex
	allMutexes map[string]*sync.Mutex // keyed by collection names
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

func (d *Driver) Read(collection string, name string, loadHere interface{}) error {
	collection = strings.TrimSpace(collection)
	name = strings.TrimSpace(name)
	if collection == "" {
		return fmt.Errorf("Received an empty collection name; a non-empty collection name was expected to be read from!")
	}
	if name == "" {
		return fmt.Errorf("Received an empty name; a non-empty target file name was expected to be read from!")
	}

	filePath := filepath.Join(d.dbDir, collection, name)
	_, err := getDBFileInfo(filePath)
	if err != nil {
		return err
	}
	
	filePath = filePath + ".json"
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &loadHere)
	return err
}

func (d* Driver) getOrCreateCollectionMutex(collection string) *sync.Mutex {
	d.myMutex.Lock()
	defer d.myMutex.Unlock()
	collectionMutex, collectionMutexExists := d.allMutexes[collection]
	if collectionMutexExists {
		return collectionMutex
	}
	collectionMutex = &sync.Mutex{}
	d.allMutexes[collection] = collectionMutex
	return collectionMutex
}

func (d* Driver) Write(collection string, name string, data interface{}) error {
	collection = strings.TrimSpace(collection)
	name = strings.TrimSpace(name)
	if collection == "" {
		return fmt.Errorf("Received an empty collection name; a non-empty collection name was expected to be written into!")
	}
	if name == "" {
		return fmt.Errorf("Received an empty name while inserting data; a non-empty target file name was expected to be written to!")
	}

	myMutex := d.getOrCreateCollectionMutex(collection)
	myMutex.Lock()
	defer myMutex.Unlock()

	insertFilepath := filepath.Join(d.dbDir, collection, name + ".json")
	tempFilepath := filepath.Join(d.dbDir, collection, name + ".tmp")
	err := os.MkdirAll(filepath.Join(d.dbDir, collection), 0755)
	if err != nil {
		return err
	}
	fileContent, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(tempFilepath, fileContent, 0666)
	if err != nil {
		return err
	}
	err = os.Rename(tempFilepath, insertFilepath)
	return err
}

func doesDBExist(cleanedDir string) bool {
	_, err := os.Stat(cleanedDir)
	return !os.IsNotExist(err)
}

func getDBFileInfo(filePath string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(filepath.Clean(filePath))
	if os.IsNotExist(err) {
		fileInfo, err = os.Stat(filepath.Clean(filePath + ".json"))
	}
	return fileInfo, err
}