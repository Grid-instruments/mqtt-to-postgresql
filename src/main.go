package main

import (
	"fmt"
	c "github.com/Grid-instruments/mqtt-to-postgresql/src/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"strings"
	"time"
)

var flag bool = false

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	//fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	topic := msg.Topic()
	payload := msg.Payload()
	if strings.Compare(string(payload), "\n") > 0 {
		fmt.Printf("TOPIC: %s\n", topic)
		fmt.Printf("MSG: %s\n", payload)
	}

	if strings.Compare("bye\n", string(payload)) == 0 {
		fmt.Println("exitting")
		flag = true
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}

func sub(client mqtt.Client, topic string) {
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	fmt.Println("Subscribed to topic: ", topic)
}

func sub2(client mqtt.Client, topic string) {
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", topic)
}

func main() {
	// Read the configuration file
	var configuration = c.ReadDB()

	//fmt.Println("Database is\t", configuration.Database.DBName)
	//fmt.Println("Database is2\t", viper.GetString("database.dbname"))

	//Set up mqtt client
	var broker = configuration.Mqtt.Broker
	var port = configuration.Mqtt.Port
	fmt.Printf("Connecting to: tcp://%s:%d\n", broker, port)
	client := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port)).
		SetClientID("go-simple").
		SetDefaultPublishHandler(messagePubHandler).
		SetOnConnectHandler(connectHandler).
		SetConnectionLostHandler(connectLostHandler),
	)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("ref/#", 1, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for flag == false {
		time.Sleep(1 * time.Second)
	}

	if token := client.Unsubscribe("ref/#"); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	client.Disconnect(250)
}
