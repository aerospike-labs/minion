package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	daemon "github.com/cstivers78/go-daemon"
	handlers "github.com/gorilla/handlers"
	rpc "github.com/gorilla/rpc/v2"
	jsonrpc "github.com/gorilla/rpc/v2/json"
)

var (
	listen     string = "0.0.0.0:9090"
	rootPath   string = currentDir()
	pidFile    string = "log/minion.pid"
	logFile    string = "log/minion.log"
	accessFile string = "log/minion-access.log"
	quiet      bool   = false
)

func checkFile(file string) string {

	var err error = nil

	if !path.IsAbs(file) {
		file = path.Join(rootPath, file)
	}

	_, err = os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			dir := path.Dir(file)
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Panic(err)
			}
		} else {
			log.Panic(err)
		}
	}

	return file
}

func checkDir(dir string) string {

	var err error = nil

	if !path.IsAbs(dir) {
		dir = path.Join(rootPath, dir)
	}

	_, err = os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Panic(err)
			}
		} else {
			log.Panic(err)
		}
	}

	return dir
}

func checkServices(ctx *ServiceContext) {

	servicesDir := checkDir(filepath.Join(rootPath, "svc"))
	servicesList, err := ioutil.ReadDir(servicesDir)
	if err != nil {
		log.Panic(err)
	}
	for _, svcDir := range servicesList {
		if svcDir.IsDir() {

			svcFile := filepath.Join(servicesDir, svcDir.Name(), "service.json")
			println("svcFile", svcFile)
			_, err = os.Stat(svcFile)
			if err == nil {
				svcData, err := ioutil.ReadFile(svcFile)
				if err == nil {
					var svc ServiceInstall
					err := json.Unmarshal(svcData, &svc)
					if err == nil {
						ctx.Registry[svc.Id] = &svc
					}
				}
			}
		}
	}
}

func main() {

	// error
	var err error

	// parse arguments
	flag.StringVar(&listen, "listen", listen, "Listening address and port for the service.")
	flag.StringVar(&pidFile, "pid", pidFile, "Path to PID file.")
	flag.StringVar(&logFile, "log", logFile, "Path to Log file.")
	flag.StringVar(&accessFile, "access", accessFile, "Path to access log file.")
	flag.StringVar(&rootPath, "root", rootPath, "Path to minion root.")
	flag.BoolVar(&quiet, "quiet", quiet, "If enabled, then do not send output to console.")
	flag.Parse()

	command := ""
	if flag.NArg() == 1 {
		command = flag.Arg(0)
	}

	// daemon signal handlers
	daemon.AddCommand(daemon.StringFlag(&command, "quit"), syscall.SIGQUIT, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "stop"), syscall.SIGTERM, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "reload"), syscall.SIGHUP, signalHup)

	os.Setenv("GOPATH", filepath.Join(rootPath, "go"))
	os.Setenv("PATH", os.Getenv("PATH")+":"+filepath.Join(rootPath, "go", "bin"))

	// check files
	pidFile = checkFile(pidFile)
	logFile = checkFile(logFile)
	accessFile = checkFile(accessFile)

	// daemon context
	ctx := &daemon.Context{
		PidFileName: pidFile,
		PidFilePerm: 0755,
		LogFileName: logFile,
		LogFilePerm: 0755,
		WorkDir:     rootPath,
		Umask:       027,
		Args:        []string{},
	}

	if len(daemon.ActiveFlags()) > 0 {
		d, err := ctx.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return
	}

	d, err := ctx.Reborn()
	if err != nil {
		log.Fatalln(err)
	}
	if d != nil {
		return
	}
	defer ctx.Release()

	// open access log
	accessLog, err := os.OpenFile(accessFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Panic("error opening access log: %v", err)
	}
	defer accessLog.Close()

	// services contexts
	serviceContext := &ServiceContext{
		Registry: map[string]*ServiceInstall{},
	}

	// export services
	rpcServer := rpc.NewServer()
	rpcServer.RegisterCodec(jsonrpc.NewCodec(), "application/json")
	rpcServer.RegisterService(serviceContext, "Service")

	// routes
	httpRouter := http.NewServeMux()
	httpRouter.Handle("/rpc", handlers.CombinedLoggingHandler(accessLog, rpcServer))

	// server
	httpServer := &http.Server{
		Addr:           listen,
		Handler:        httpRouter,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	checkServices(serviceContext)

	// start
	go func() {
		log.Printf("Starting HTTP on http://%s\n", listen)
		log.Panic(httpServer.ListenAndServe())
	}()

	// daemon handles signals
	if err = daemon.ServeSignals(); err != nil {
		log.Panic(err)
	}

	// exit handled by signal handlers
	halt := make(chan bool)
	<-halt
}

func currentDir() string {
	s, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return s
}

func signalQuit(s os.Signal) error {
	// logInfo("Signal QUIT Received %v", sig)
	os.Exit(0)
	return nil
}

func signalTerm(s os.Signal) error {
	// logInfo("Signal TERM Received %v", sig)
	os.Exit(0)
	return nil
}

func signalHup(s os.Signal) error {
	// logInfo("Signal HUP Received %v", s)
	return nil
}
