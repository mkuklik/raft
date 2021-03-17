package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	raft "github.com/mkuklik/raft/raft"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type StateM struct{}
type StateEvent struct{}

func (sm StateM) Apply(event raft.StateMachineEvent) bool {
	return false
}

func (sm StateM) Current() interface{} {
	return StateM{}
}

func main() {

	var cviper = viper.New()

	flag.String("log", "prod", "logger type: none, dev, prod*")
	flag.String("db", "pg", "database backend: pg")
	flag.Bool("bootstrap", false, "bootstrap cluster")
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

	sm := StateM{}
	mgr := raft.NewManager(&config, sm)
	mgr.Run(":1234")
}
