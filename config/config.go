package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"

	"mongo-elastic/fields"
)

type Config struct {
	MongoURL   string `yaml:"mongoURL"`
	ElasticURL string `yaml:"elasticURL"`
	// Embedded SyncMapping
	Databases []DatabaseMapping `yaml:"databases"`
}

type SyncMapping struct {
	Databases []DatabaseMapping `yaml:"databases"`
}

type DatabaseMapping struct {
	Name        string              `yaml:"name"`
	Collections []CollectionMapping `yaml:"collections"`
}

type CollectionMapping struct {
	Name   string     `yaml:"name"`
	Fields []fields.M `yaml:"fields"`
}

// FromYamlFile decodes the yaml content at the given file path into config.
// TODO: Add validation
func FromYamlFile(filePath string, config *Config) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(b, config)
}
