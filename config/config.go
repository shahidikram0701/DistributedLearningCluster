package config

import (
	"encoding/json"
	"log"
	"os"
)

type Configuration struct {
	LoggerPort                   int    `json:"LoggerPort"`
	LogToFile                    bool   `json:"LogToFile"`
	IntroducerAddress            string `json:"IntroducerAddress"`
	IntroducerPort               int    `json:"IntroducerPort"`
	UdpServerPort                int    `json:"UdpServerPort"`
	CoordinatorServiceLoggerPort int    `json:"CoordinatorServiceLoggerPort"`
	FailureDetectorPort          int    `json:"FailureDetectorPort"`
	NumOfCoordinators            int    `json:"NumOfCoordinators"`
	NumOfReplicas                int    `json:"NumOfReplicas"`
	CoordinatorServiceSDFSPort   int    `json:"CoordinatorServiceSDFSPort"`
	ChunkSize                    int    `json:"ChunkSize"`
	DataRootFolder               string `json:"DataRootFolder"`
	DataNodeServiceSDFSPort      int    `json:"DataNodeServiceSDFSPort"`
	WriteQuorum                  int    `json:"WriteQuorum"`
	ReadQuorum                   int    `json:"ReadQuorum"`
	NumRetriesPerOperation       int    `json:"NumRetriesPerOperation"`
	SDFSDataFolder               string `json:"SDFSDataFolder"`
	ReplicaRecoveryInterval      int    `json:"ReplicaRecoveryInterval"`
	OutputDataFolder             string `json:"OutputDataFolder"`
	CoordinatorSyncTimer         int    `json:"CoordinatorSyncTimer"`
	NumOfWorkersPerModel         int    `json:"NumOfWorkersPerModel"`
	SchedulerPort                int    `json:"SchedulerPort"`
	ModelsDataFolder             string `json:"ModelsDataFolder"`
	SDFSModelsFolder             string `json:"SDFSModelsFolder"`
	WorkerPort                   int    `json:"WorkerPort"`
	SchedulerPollInterval        int    `json:"SchedulerPollInterval"`
	TaskExecutionTimeout         int    `json:"TaskExecutionTimeout"`
}

func GetConfig(configFilePath string, params ...string) Configuration {
	var config Configuration
	configFile, err := os.Open(configFilePath)
	defer configFile.Close()
	if err != nil {
		log.Panicf("Error getting config: %s", err)
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)

	env := "dev"
	if len(params) > 0 && env == "dev" {
		env = params[0]
	}

	if env == "dev" {
		config.IntroducerAddress = "192.168.64.6"
	}

	return config
}
