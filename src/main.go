package main

import (
	"fmt"
	c "github.com/Grid-instruments/mqtt-to-postgresql/src/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}

func main() {
	// Set the file name of the configurations file
	viper.SetConfigName("config")
	// Add the path to look for the configurations file
	viper.AddConfigPath(".")
	// Set the file type of the configurations file
	viper.SetConfigType("yaml")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	var configuration c.Configurations

	// Read in the configurations file and check for errors
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s\n", err)
		panic(err)
	}

	// set default values
	viper.SetDefault("database.dbname", "test")

	if err := viper.Unmarshal(&configuration); err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
	}

	fmt.Println("Database is\t", configuration.Database.DBName)
	fmt.Println("Database is\t", viper.GetString("database.dbname"))

	//
}
