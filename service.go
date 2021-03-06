package main

import (
	"github.com/aerospike-labs/minion/service"

	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ----------------------------------------------------------------------------
//
// Types
//
// ----------------------------------------------------------------------------

type ServiceContext struct {
	SendEventMessage func(data, event, id string)
	Registry         map[string]*ServiceInstall
}

type ServiceInstall struct {
	Id     string                 `json:"id"`
	URL    string                 `json:"url"`
	Params map[string]interface{} `json:"params"`
}

// ----------------------------------------------------------------------------
//
// Bundles Methods
//
// ----------------------------------------------------------------------------

func (self *ServiceContext) getenv(serviceId string, serviceUrl string) []string {

	etcPath := filepath.Join(rootPath, "etc")
	svcPath := filepath.Join(rootPath, "svc", serviceId)
	goRoot := filepath.Join(rootPath, "go")
	goBin := filepath.Join(goRoot, "bin")

	env := []string{}
	env = append(env, "GOPATH="+svcPath)
	env = append(env, "GOROOT="+goRoot)
	env = append(env, "PATH="+os.Getenv("PATH")+":"+goBin)
	env = append(env, "SERVICE_ID="+serviceId)
	env = append(env, "SERVICE_URL="+serviceUrl)
	env = append(env, "SERVICE_PATH="+svcPath)
	env = append(env, "MINION_ROOT="+rootPath)
	env = append(env, "CONFIG_PATH="+etcPath)
	return env
}

// List Bundles
func (self *ServiceContext) List(req *http.Request, args *struct{}, res *map[string]*ServiceInstall) error {
	*res = self.Registry
	return nil
}

// Install a Bundle
func (self *ServiceContext) Install(req *http.Request, svc *ServiceInstall, res *string) error {

	var err error = nil

	log.Printf("info: installing id=%s url=%s params=%#v\n", svc.Id, svc.URL, svc.Params)

	if _, exists := self.Registry[svc.Id]; exists {
		log.Printf("error: Service found: %s", svc.Id)
		return service.Exists
	}

	// make sure the svc path exists
	svcPath := filepath.Join(rootPath, "svc", svc.Id)
	os.MkdirAll(svcPath, 0755)

	// env
	env := self.getenv(svc.Id, svc.URL)

	// download the service
	get := exec.Command("go", "get", "-u", svc.URL)
	log.Printf("info: executing: %s %s\n", get.Path, strings.Join(get.Args, " "))
	get.Env = env
	get.Dir = svcPath
	getOut, err := get.CombinedOutput()
	if err != nil {
		log.Printf("error: %s\n", err.Error())
		return err
	} else {
		if len(getOut) > 0 {
			log.Printf("info: out: %x\n", string(getOut))
		}
	}

	// binPath := filepath.Join("service")
	build := exec.Command("go", "build", "-o", "service", svc.URL)
	log.Printf("info: executing: %s %s\n", build.Path, strings.Join(build.Args, " "))
	build.Env = env
	build.Dir = svcPath
	buildOut, err := build.CombinedOutput()
	if err != nil {
		log.Printf("error: %s\n", err.Error())
		return err
	} else {
		if len(buildOut) > 0 {
			log.Printf("info: out: %x\n", buildOut)
		}
	}

	// write the url file
	jsonFile := filepath.Join(svcPath, "service.json")
	log.Printf("info: loading service.json: %s\n", jsonFile)
	jsonData, err := json.Marshal(svc)
	if err != nil {
		log.Printf("error: loading service.json: %s\n", err.Error())
		return err
	}
	ioutil.WriteFile(jsonFile, jsonData, 0755)

	// write the url file
	envFile := filepath.Join(svcPath, "service.env")
	envData := &bytes.Buffer{}
	for _, e := range env {
		envData.WriteString("export ")
		envData.WriteString(e)
		envData.WriteRune('\n')
	}
	ioutil.WriteFile(envFile, envData.Bytes(), 0755)

	// run "install" command
	log.Printf("info: installing\n")
	if err = self.run(svc.Id, "install", svc.Params, res); err != nil {
		log.Printf("error: installing: %s\n", err.Error())
		return err
	}

	self.Registry[svc.Id] = svc

	// *res = string(out)
	return err
}

// Remove a Bundle
func (self *ServiceContext) Remove(req *http.Request, serviceId *string, res *string) error {

	var err error = nil

	svc, exists := self.Registry[*serviceId]
	if !exists {
		log.Printf("error: Service not found: %s\n", serviceId)
		return service.NotFound
	}

	// run "remove" command
	if err = self.run(svc.Id, "remove", map[string]interface{}{}, res); err != nil {
		return err
	}

	delete(self.Registry, svc.Id)

	svcPath := filepath.Join(rootPath, "svc", svc.Id)

	// clean up

	cmd := exec.Command("go", "clean", svc.URL)
	cmd.Env = self.getenv(*serviceId, svc.URL)
	cmd.Dir = svcPath
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("error: %s\n", err.Error())
		return err
	} else {
		if len(out) > 0 {
			log.Printf("info: out: %x\n", out)
		}
	}

	srcPath := filepath.Join(rootPath, "src", svc.URL)
	if err = os.RemoveAll(srcPath); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("error: %s\n", err.Error())
			return err
		}
	}

	binPath := filepath.Join(rootPath, "bin", svc.Id)
	if err = os.RemoveAll(binPath); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("error: %s\n", err.Error())
			return err
		}
	}

	if err = os.RemoveAll(svcPath); err != nil {
		if !os.IsNotExist(err) {
			log.Printf("error: %s\n", err.Error())
			return err
		}
	}

	return err
}

// Check Existence of a Service
func (self *ServiceContext) Exists(req *http.Request, serviceId *string, res *bool) error {

	if _, exists := self.Registry[*serviceId]; exists {
		*res = true
	} else {
		*res = false
	}

	return nil
}

// Status of the Service
func (self *ServiceContext) Status(req *http.Request, serviceId *string, res *string) error {
	if _, exists := self.Registry[*serviceId]; !exists {
		return service.NotFound
	}
	return self.run(*serviceId, "status", map[string]interface{}{}, res)
}

// Start the Service
func (self *ServiceContext) Start(req *http.Request, serviceId *string, res *string) error {
	if _, exists := self.Registry[*serviceId]; !exists {
		return service.NotFound
	}
	return self.run(*serviceId, "start", map[string]interface{}{}, res)
}

// Stop the Service
func (self *ServiceContext) Stop(req *http.Request, serviceId *string, res *string) error {
	if _, exists := self.Registry[*serviceId]; !exists {
		return service.NotFound
	}
	return self.run(*serviceId, "stop", map[string]interface{}{}, res)
}

// Stats of the Service
func (self *ServiceContext) Stats(req *http.Request, serviceId *string, res *map[string]interface{}) error {
	var out string = ""

	if _, exists := self.Registry[*serviceId]; !exists {
		return service.NotFound
	}

	err := self.run(*serviceId, "stats", map[string]interface{}{}, &out)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(out), res)
	return err
}

// Run a Service Command
func (self *ServiceContext) run(serviceId string, commandName string, params map[string]interface{}, res *string) error {

	var err error = nil
	var serviceUrl string = ""

	svc, exists := self.Registry[serviceId]
	if exists {
		serviceUrl = svc.URL
	}

	svcPath := filepath.Join(rootPath, "svc", serviceId)
	binPath := filepath.Join(svcPath, "service")
	cmd := exec.Command(binPath, commandName)
	log.Printf("info: executing: %s %s\n", cmd.Path, strings.Join(cmd.Args, " "))
	cmd.Dir = svcPath
	cmd.Env = self.getenv(serviceId, serviceUrl)

	b, err := json.Marshal(params)
	if err != nil {
		log.Printf("error: %s", err.Error())
		return err
	} else {
		cmd.Stdin = bytes.NewReader(b)
	}
	log.Printf("info: executing: < %s\n", b)

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("error: executing: %s\n", err.Error())
		if out != nil && len(out) > 0 {
			log.Printf("error: out: %x\n", out)
			log.Printf("error: out: %q\n", string(out))
		}
	} else {
		*res = string(out)
		log.Printf("info: out: %x\n", out)
	}

	return err
}
