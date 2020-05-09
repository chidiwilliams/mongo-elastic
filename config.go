package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type config struct {
	MongoURL   string           `yaml:"mongoURL"`
	ElasticURL string           `yaml:"elasticURL"`
	Databases  []databaseConfig `yaml:"databases"`
}

type databaseConfig struct {
	Name        string             `yaml:"name"`
	Collections []collectionConfig `yaml:"collections"`
}

type collectionConfig struct {
	Name   string `yaml:"name"`
	Fields []struct {
		Name string `yaml:"name"`
	} `yaml:"fields"`
}

func parseConfig(filePath string, config *config) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, config)
}
