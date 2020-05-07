package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type config struct {
	MongoURL   string `yaml:"mongoURL"`
	ElasticURL string `yaml:"elasticURL"`
	Databases  []struct {
		Name        string `yaml:"name"`
		Collections []struct {
			Name   string `yaml:"name"`
			Fields []struct {
				Name string `yaml:"name"`
			} `yaml:"fields"`
		} `yaml:"collections"`
	} `yaml:"databases"`
}

func main() {
	configPtr := flag.String("config", "config.yaml", "Configuration file")
	flag.Parse()

	c := config{}
	err := parseConfig(*configPtr, &c)
	if err != nil {
		log.Fatal(fmt.Errorf("error parsing config file: %w", err))
	}
}

func parseConfig(filePath string, config *config) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, config)
}
