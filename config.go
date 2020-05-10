package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"

	"mongo-elastic/fields"
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
	Name string `yaml:"name"`
	// TODO: Validate field names input (dot-syntax)
	Fields []fields.M `yaml:"fields"`
}

func parseConfig(filePath string, config *config) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, config)
}
