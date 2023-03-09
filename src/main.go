package main

import (
	"fmt"
	c "github.com/Grid-instruments/mqtt-to-postgresql/src/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math"
	"os"
	"strings"
	"time"
)

// Measurement gorm.Model definition
type Measurement struct {
	ID       uint `gorm:"primaryKey"`
	NodeID   string
	Phi      float64
	Phi2     float64
	Phi3     float64
	Xm2      float64
	Freq     float64
	T        float64
	Dt       time.Time
	Report   int
	Verified bool
}

var globalDb *gorm.DB

var topic string

var ch = make(chan Measurement)

var flag bool = false

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	//fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	topic := msg.Topic()
	payload := msg.Payload()
	if strings.Compare(string(payload), "\n") > 0 {

		msg := parseString(string(payload))
		// If message is not empty
		if msg != (Measurement{}) {
			fmt.Printf("TOPIC: %s\n", topic)
			fmt.Printf("MSG: %s\n", payload)
			// Split topic to get NodeID
			parts := strings.Split(topic, "/")
			if len(parts) < 3 {
				panic("not enough slashes")
			}
			NodeID := parts[1]
			msg.NodeID = NodeID

			// Send message to channel
			//select {
			//case ch <- msg:
			//default:
			//	fmt.Println("Channel is full")
			//}

			// Insert message to database
			err := insertMeasurement(globalDb, msg)
			if err != nil {
				return
			}
		}
	}

	if strings.Compare("bye\n", string(payload)) == 0 {
		fmt.Println("exiting")
		flag = true
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("MQTT connected")
	subscribe(client, topic)
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("MQTT connect lost: %v\n", err)
}

var reconnectHandler mqtt.ReconnectHandler = func(client mqtt.Client, options *mqtt.ClientOptions) {
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			fmt.Println("Connection lost, reconnecting...")
			time.Sleep(5 * time.Second)
		} else {
			// resubscribe to topics
			subscribe(client, "dev/#")
			break
		}
	}
}

func subscribe(client mqtt.Client, topic string) {
	if token := client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	fmt.Println("Subscribed to topic: ", topic)
}

func unsubscribe(client mqtt.Client, topic string) {
	if token := client.Unsubscribe(topic); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	fmt.Println("Unsubscribed from topic: ", topic)
}

func parseString(input string) Measurement {
	// Check if the input string contains "SinglePhaseReportData"
	//if !strings.Contains(input, "SinglePhaseReportData") {
	//	return Measurement{}
	//}

	index := strings.Index(input, "SinglePhaseReportData")
	if index == -1 {
		return Measurement{}
	}

	result := input[index+len("SinglePhaseReportData"):]

	var phi, xm2, freq, t float64
	var dt time.Time
	var verified bool
	var report int
	var year, month, day, hour, minute, second int

	_, err := fmt.Sscanf(result, "(phi: %f, xm2: %f, freq: %f, t: %f, dt: %d. %d. %d %d:%d:%d %d (verified: %t))",
		&phi, &xm2, &freq, &t,
		&year, &month, &day, &hour, &minute, &second,
		&report, &verified)
	if err != nil {
		panic(err)
	}
	if verified != true {
		return Measurement{}
	}

	dt = time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)

	switch report {
	case 9, 19, 29, 39, 49:
		phi2 := phi + (2 * math.Pi / 3)
		phi3 := phi - (2 * math.Pi / 3)
		return Measurement{
			Phi:      phi,
			Phi2:     phi2,
			Phi3:     phi3,
			Xm2:      xm2,
			Freq:     freq,
			T:        t,
			Dt:       dt,
			Report:   report,
			Verified: verified,
		}
	default:
		return Measurement{}
	}

}

func createTableIfNotExists(db *gorm.DB) error {
	// Drop the measurements table using a raw SQL query
	/*result := db.Exec("DROP TABLE IF EXISTS measurements")
	if result.Error != nil {
		panic(result.Error)
	}
	// Print a success message
	fmt.Println("Table measurements has been dropped successfully!")*/

	// Check if "measurements" table exists
	/*tableExists := db.Migrator().HasTable("measurements")
	if tableExists {
		fmt.Println("Table 'measurements' already exists.")
		return nil
	}*/

	// Create the "measurements" table
	err := db.AutoMigrate(&Measurement{})
	if err != nil {
		fmt.Println("Error creating table 'measurements': ", err)
		return err
	}
	fmt.Println("Table 'measurements' created successfully.")

	return nil
}

func insertMeasurement(db *gorm.DB, measurement Measurement) error {
	result := db.Create(&measurement)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func insertMeasurementsChannel(db *gorm.DB) error {
	for msg := range ch {
		result := db.Create(&msg)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func deleteOldData(db *gorm.DB) error {
	ticker := time.NewTicker(5 * time.Minute)

	for range ticker.C {
		// Compute the timestamp 5 minutes ago
		fiveMinutesAgo := time.Now().Add(-5 * time.Minute)

		// Delete rows older than the computed timestamp
		result := db.Where("dt < ?", fiveMinutesAgo).Delete(&Measurement{})
		if result.Error != nil {
			return result.Error
		}
		fmt.Println("Deleted rows older than 5 minutes")
	}

	return nil
}

func main() {
	// Read the configuration file
	var configuration = c.ReadDB()

	topic = configuration.Mqtt.Topic

	fmt.Println("Trying to connect to database")
	// Set up database connection
	/*dsn := "host=" + configuration.Database.DBHost + " user=" + configuration.Database.DBUser +
	" password=" + configuration.Database.DBPassword + " dbname=" + configuration.Database.DBName +
	" port=" + configuration.Database.DBPort + " sslmode=" + configuration.Database.DBSSLMode + " TimeZone=" + configuration.Database.DBTimezone*/
	// Set-up database
	dsn2 := "host=195.201.130.247 user=test password=a63Nd2i5KCm dbname=mydb port=5432 sslmode=disable TimeZone=UTC"
	db, err := gorm.Open(postgres.Open(dsn2), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Error),
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		panic("failed to connect database")
	}
	fmt.Println("Database connected")

	// Create the "measurements" table if it doesn't exist
	err = createTableIfNotExists(db)
	if err != nil {
		panic(err)
	}
	fmt.Println("Database created")

	globalDb = db
	fmt.Println("Starting go routine")
	//go func() {
	//	err := insertMeasurementsChannel(db)
	//	if err != nil {
	//		panic(err)
	//	}
	//}()

	go func() {
		err := deleteOldData(db)
		if err != nil {
			panic(err)
		}
	}()
	fmt.Println("Go routine started")

	//--------------------------------------------------------------
	//							Setup MQTT
	//--------------------------------------------------------------
	var broker = configuration.Mqtt.Broker
	var port = configuration.Mqtt.Port
	fmt.Printf("Connecting to: tcp://%s:%d\n", broker, port)
	client := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port)).
		SetClientID("go-simple").
		SetDefaultPublishHandler(messagePubHandler).
		SetOnConnectHandler(connectHandler).
		SetConnectionLostHandler(connectLostHandler).
		SetResumeSubs(true).
		SetKeepAlive(10),
	)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	//subscribe(client, "dev/#")

	/*if token := client.Subscribe("dev/#", 1, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}*/

	for flag == false {
		time.Sleep(1 * time.Second)
	}

	if token := client.Unsubscribe("dev/#"); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	close(ch)

	//client.Disconnect(250)
}
