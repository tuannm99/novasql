package internal

import (
	"fmt"

	"github.com/spf13/viper"
)

type NovaSqlConfig struct {
	AppName string `mapstructure:"app_name"`

	Storage struct {
		Mode     string `mapstructure:"mode"`
		Workdir  string `mapstructure:"workdir"`
		PageSize int    `mapstructure:"page_size"`
	} `mapstructure:"storage"`

	Server struct {
		Port  int  `mapstructure:"port"`
		Debug bool `mapstructure:"debug"`
	} `mapstructure:"server"`
}

func LoadConfig(path string) (*NovaSqlConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg NovaSqlConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
