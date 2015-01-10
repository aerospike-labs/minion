package main

import (
	"github.com/aerospike-labs/minion/service"

	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
)

// ----------------------------------------------------------------------------
//
// Types
//
// ----------------------------------------------------------------------------

type ServiceContext struct {
	SendEventMessage func(data, event, id string)
	Registry         map[string]string
}

type ServiceInstall struct {
	Name   string                 `json:"name"`
	URL    string                 `json:"url"`
	Params map[string]interface{} `json:"params"`
}

// ----------------------------------------------------------------------------
//
// Bundles Methods
//
// ----------------------------------------------------------------------------

// List Bundles
func (self *ServiceContext) List(req *http.Request, args *struct{}, res *map[string]string) error {
	*res = self.Registry
	return nil
}

// Install a Bundle
func (self *ServiceContext) Install(req *http.Request, args *ServiceInstall, res *string) error {

	var err error = nil

	if _, ok := self.Registry[args.Name]; ok {
		return service.Exists
	}

	get := exec.Command("go", "get", "-u", args.URL)
	getOut, err := get.CombinedOutput()
	println("out: ", string(getOut))
	if err != nil {
		println("error: ", err.Error())
		return err
	}

	binPath := filepath.Join(rootPath, "bin", args.Name)
	build := exec.Command("go", "build", "-o", binPath, args.URL)
	buildOut, err := build.CombinedOutput()
	println("out: ", string(buildOut))
	if err != nil {
		println("error: ", err.Error())
		return err
	}

	self.Registry[args.Name] = args.URL

	// create service directory
	svcPath := filepath.Join(rootPath, "svc", args.Name)
	os.MkdirAll(svcPath, 0755)

	// run "install" command
	if err = self.run(args.Name, "install", args.Params, res); err != nil {
		return err
	}

	// *res = string(out)
	return err
}

// Remove a Bundle
func (self *ServiceContext) Remove(req *http.Request, serviceName *string, res *string) error {

	var err error = nil

	serviceURL, exists := self.Registry[*serviceName]
	if !exists {
		return service.NotFound
	}

	delete(self.Registry, *serviceName)

	// run "remove" command
	if err = self.run(*serviceName, "remove", map[string]interface{}{}, res); err != nil {
		return err
	}

	// clean up

	cmd := exec.Command("go", "clean", serviceURL)
	out, err := cmd.CombinedOutput()
	println("out: ", string(out))
	if err != nil {
		println("error: ", err.Error())
		return err
	}

	srcPath := filepath.Join(rootPath, "src", serviceURL)
	if err = os.RemoveAll(srcPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	binPath := filepath.Join(rootPath, "bin", *serviceName)
	if err = os.RemoveAll(binPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	svcPath := filepath.Join(rootPath, "svc", *serviceName)
	if err = os.RemoveAll(svcPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// !!! Dangerous
	// srcPath := filepath.Join("src", serviceUrl)
	// if err = os.RemoveAll(srcPath); err != nil {
	// 	if !os.IsNotExist(err) {
	// 		return err
	// 	}
	// }

	return err
}

// Check Existence of a Service
func (self *ServiceContext) Exists(req *http.Request, serviceName *string, res *bool) error {

	if _, ok := self.Registry[*serviceName]; ok {
		*res = true
	} else {
		*res = false
	}

	return nil
}

// Status of the Service
func (self *ServiceContext) Status(req *http.Request, serviceName *string, res *string) error {
	return self.run(*serviceName, "status", map[string]interface{}{}, res)
}

// Start the Service
func (self *ServiceContext) Start(req *http.Request, serviceName *string, res *string) error {
	return self.run(*serviceName, "start", map[string]interface{}{}, res)
}

// Stop the Service
func (self *ServiceContext) Stop(req *http.Request, serviceName *string, res *string) error {
	return self.run(*serviceName, "stop", map[string]interface{}{}, res)
}

// Stats of the Service
func (self *ServiceContext) Stats(req *http.Request, serviceName *string, res *map[string]int) error {
	var out string = ""

	err := self.run(*serviceName, "stats", map[string]interface{}{}, &out)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(out), res)
	return err
}

// Run a Service Command
func (self *ServiceContext) run(serviceName string, commandName string, params map[string]interface{}, res *string) error {
	var err error = nil

	serviceUrl, serviceExists := self.Registry[serviceName]
	if !serviceExists {
		return service.NotFound
	}

	binPath := filepath.Join(rootPath, "bin", serviceName)

	cmd := exec.Command(binPath, commandName)
	cmd.Env = append(cmd.Env, "GOPATH="+rootPath)
	cmd.Env = append(cmd.Env, "SERVICE_NAME="+serviceName)
	cmd.Env = append(cmd.Env, "SERVICE_URL="+serviceUrl)
	cmd.Env = append(cmd.Env, "SERVICE_PATH="+filepath.Join(rootPath, "svc", serviceName))

	b, err := json.Marshal(params)
	if err != nil {
		return err
	} else {
		cmd.Stdin = bytes.NewReader(b)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	*res = string(out)
	return err
}
