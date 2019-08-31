package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bhmj/sqlsync/config"
	"github.com/bhmj/sqlsync/syncer"
)

func main() {

	configFile := flag.String("config", "", "path to config file")
	flag.Parse()
	if configFile == nil || *configFile == "" || !FileExists(*configFile) {
		fmt.Fprintf(os.Stderr, "Usage: sqlsync [params] \n")
		flag.PrintDefaults()
		return
	}

	settings, err := config.ReadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ReadConfig: %s\n", err.Error())
		return
	}

	errs := make(chan error, 32)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errs <- fmt.Errorf("%s", <-c)
	}()

	shutdown := make(chan bool, 1)
	go func() {
		fmt.Printf("\nbegin shutdown: %v\n", <-errs)
		shutdown <- true
	}()

	// init RVs
	for i := 0; i < len(settings.Sync); i++ {
		syncer.Init(&settings.Sync[i])
	}

	jobs := make(chan int)

	for i := 0; i < len(settings.Sync); i++ {
		fmt.Printf("adding %s (%s)\n", *settings.Sync[i].Origin, settings.Sync[i].Period.Duration)
		go func(sync int) {
			for {
				jobs <- sync
				time.Sleep(settings.Sync[sync].Period.Duration)
			}
		}(i)
	}
	fmt.Println("started")
	for {
		select {
		case <-shutdown:
			fmt.Println("terminated")
			os.Exit(0)
		case sync := <-jobs:
			syncer.DoSync(&settings.Sync[sync])
		}
	}
}

// FileExists ...
func FileExists(fname string) bool {
	_, err := os.Stat("/path/to/whatever")
	return os.IsNotExist(err)
}
