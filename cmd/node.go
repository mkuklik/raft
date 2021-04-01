package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	raft "github.com/mkuklik/raft/raft"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type StateM struct{}

func (sm StateM) Apply(event []byte) error {
	return fmt.Errorf("Not implemented yet")
}

func (sm StateM) Snapshot() ([]byte, error) {
	return nil, fmt.Errorf("Not implemented yet")
}

func main() {

	var cviper = viper.New()

	flag.String("log", "prod", "logger type: none, dev, prod*")
	flag.String("db", "pg", "database backend: pg")
	addr := flag.String("addr", ":1234", "address")
	flag.String("bootstrap", "", "address to bootstrap cluster")
	logFile := flag.String("logfile", "", "file where log entries are persisted")
	clean := flag.Bool("clean", false, "start clean, i.e. new log file etc.")
	configFile := flag.String("config", "", "config file")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	cviper.BindPFlags(pflag.CommandLine)
	cviper.SetConfigName("config") // name of config file (without extension)
	cviper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	cviper.AddConfigPath(".")      // optionally look for config in the working directory
	if configFile != nil {
		cviper.AddConfigPath(*configFile)
	}
	err := cviper.ReadInConfig() // Find and read the config file
	if err != nil {              // Handle errors reading the config file
		// panic(fmt.Errorf("Fatal error config file: %s", err))
		log.Errorf("Fatal error config file: %s", err)
	}

	config := raft.NewConfig()
	cviper.Unmarshal(&config)

	var nodeID int = -1
	for i, p := range config.Peers {
		if p == *addr {
			nodeID = i
		}
	}
	if nodeID < 0 {
		log.Fatalf("node address, %s, is not in a peer set", *addr)
	}

	// logging
	log.SetLevel(log.DebugLevel)

	// LogEntry filename
	filepath := *logFile
	if *logFile == "" {
		filepath = fmt.Sprintf("logfile.%d.raft", nodeID)
	}

	if *clean {
		// delete log file
		err := os.Remove(filepath)
		if err != nil {
			log.Fatalf("failed to clean log files, %s", err.Error())
		}
	}

	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("can't create log file %s, %e", *logFile, err.Error())
	}
	log.Infof("opened logfile %s", filepath)
	defer file.Close()

	sm := StateM{} // Some state machine
	r := raft.NewRaftNode(&config, uint32(nodeID), sm, file)

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancelFunc := context.WithCancel(context.Background())

	r.Run(ctx, *addr)

	<-termChan // Blocks here until either SIGINT or SIGTERM is received.

	cancelFunc()
}
