package logging

import (
	"io/ioutil"
	"log"
	"os"
)

var logger *log.Logger

func init() {
	logger = log.New(os.Stderr, "pbft: ", log.Lshortfile|log.Ltime|log.Lmicroseconds)
	if os.Getenv("BFT_LOG") != "1" {
		logger.SetOutput(ioutil.Discard)
	}
}

// GetLogger returns a pointer to the global logger for HotStuff
func GetLogger() *log.Logger {
	return logger
}
