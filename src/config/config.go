package config

import (
	"fmt"
	"github.com/spf13/viper"
)

func ReadDB() Configurations {
	// Set the file name of the configurations file
	viper.SetConfigName("config")
	// Add the path to look for the configurations file
	viper.AddConfigPath(".")
	// Set the file type of the configurations file
	viper.SetConfigType("yaml")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	var configuration Configurations

	// Read in the configurations file and check for errors
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s\n", err)
		panic(err)
	}

	// set default values
	viper.SetDefault("database.dbname", "test")

	if err := viper.Unmarshal(&configuration); err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
		panic(err)
	}

	return configuration
}

// Configurations exported
type Configurations struct {
	Server       ServerConfigurations
	Database     DatabaseConfigurations
	Mqtt         MqttConfigurations
	EXAMPLE_PATH string
	EXAMPLE_VAR  string
}

// ServerConfigurations exported
type ServerConfigurations struct {
	Port int
}

// DatabaseConfigurations exported
type DatabaseConfigurations struct {
	DBName     string
	DBUser     string
	DBPassword string
}

type MqttConfigurations struct {
	Broker   string
	Port     int
	ClientID string
	Username string
	Password string
	Topic    string
}
