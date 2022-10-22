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
