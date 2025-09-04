package internal

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/tuannm99/novasql/internal/storage"
)

type NovaSqlConfig struct {
	Storage struct {
		Mode     string `mapstructure:"mode"`
		File     string `mapstructure:"file"`
		PageSize int    `mapstructure:"page_size"`
	} `mapstructure:"storage"`
	Server struct {
		Port  int  `mapstructure:"port"`
		Debug bool `mapstructure:"debug"`
	} `mapstructure:"server"`
}

type Config struct {
	Mode storage.StorageMode
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
