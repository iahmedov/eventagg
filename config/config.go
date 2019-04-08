package config

import (
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/go-playground/validator.v9"
	"gopkg.in/yaml.v2"
)

type (
	Config struct {
		Version     string          `yaml:"-"`
		Server      Server          `yaml:"server" validate:"required,dive"`
		Persistence FilePersistence `yaml:"persistence" validate:"required,dive"`
		Aggregators []Aggregator    `yaml:"aggregators" validate:"dive"`
	}

	Server struct {
		Port int `yaml:"port" validate:"required,min=80,max=65535"`
	}

	FilePersistence struct {
		Dir   string `yaml:"dir"`
		Count int    `yaml:"worker_count" validate:"gte=1"`
	}

	Aggregator struct {
		Name   string                 `yaml:"name" validate:"required"`
		Alias  string                 `yaml:"alias" validate:"required"`
		Params map[string]interface{} `yaml:"params"`
	}
)

const (
	Version = "VERSION"
)

func ReadFile(configPath string) (Config, error) {
	var c Config
	raw, err := ioutil.ReadFile(configPath)
	if err != nil {
		return c, errors.Errorf("failed to read config file %s: %s", configPath, err)
	}
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &c); err != nil {
		return c, errors.Wrap(err, "yaml parse failed")
	}

	c.Version = os.Getenv(Version)
	return c, validator.New().Struct(c)
}
