package main

import (
	"io/ioutil"

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

func parseConfig(filePath string, config *config) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, config)
}
