package service

import (
	"encoding/json"
	"errors"
	"flag"
	// "fmt"
	"io/ioutil"
	"log"
	"os"
)

var (
	Exists   = errors.New("Service Exists")
	NotFound = errors.New("Service Not Found")
)

type Status int

const (
	Running Status = iota
	Stopped
	StatusUnknown
)

type Service interface {
	Install(map[string]interface{}) error
	Remove() error
	Status() (Status, error)
	Start() error
	Stop() error
	Stats() (map[string]interface{}, error)
}

func serviceError(err error) {
	if err != nil {
		println("error: " + err.Error())
		os.Exit(1)
	}
}

func Run(s Service) {

	flag.Parse()
	args := flag.Args()

	// setup logger
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if len(args) == 0 {
		println("error: missing command")
		return
	}

	cmd := args[0]
	switch cmd {
	case "install":
		// read params from stdin as JSON
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			serviceError(err)
		} else {
			var params map[string]interface{}
			err = json.Unmarshal(b, &params)
			if err != nil {
				serviceError(err)
			} else {
				serviceError(s.Install(params))
			}
		}
	case "remove":
		serviceError(s.Remove())
	case "status":
		status, err := s.Status()
		if err == nil {
			switch status {
			case Running:
				println("status: running")
			case Stopped:
				println("status: stopped")
			case StatusUnknown:
				println("status: unknown")
			}
		} else {
			serviceError(err)
		}
	case "start":
		serviceError(s.Start())
	case "stop":
		serviceError(s.Stop())
	case "stats":
		// output stats to stdout as JSON
		stats, err := s.Stats()
		if err == nil {
			b, err := json.Marshal(stats)
			if err != nil {
				serviceError(err)
			} else {
				os.Stdout.Write(b)
				os.Stdout.WriteString("\n")
			}
		} else {
			serviceError(err)
		}
	default:
		println("error: unknown command: " + cmd)
	}
}
