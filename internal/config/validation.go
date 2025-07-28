package config

import (
	"github.com/go-playground/validator/v10"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

func ValidateConfig(config *Config) error {
	return validate.Struct(config)
}