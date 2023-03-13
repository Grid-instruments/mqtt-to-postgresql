package main

import (
	"context"
	"fmt"
	c "github.com/Grid-instruments/mqtt-to-postgresql/src/config"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5"
	"math"
	"os"
	"strings"
	"time"
)

// Measurement gorm.Model definition
type Measurement struct {
	ID       uint
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

var globalDb *pgx.Conn

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
			fmt.Printf("TOPIC: %s | MSG: %s \n", topic, payload)
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

func insertMeasurement(db *pgx.Conn, measurement Measurement) error {
	// Time how long it takes to insert a row
	start := time.Now()

	// Insert the measurement into the database
	sql := "INSERT INTO measurements_insert_1 (node_id, phi, phi2, phi3, xm2, freq, t, dt, report, verified) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	_, err := db.Exec(context.Background(), sql, measurement.NodeID, measurement.Phi, measurement.Phi2, measurement.Phi3, measurement.Xm2, measurement.Freq, measurement.T, measurement.Dt, measurement.Report, measurement.Verified)
	if err != nil {
		return err
	}

	elapsed := time.Since(start)
	fmt.Printf("Inserted row in %s\n", elapsed)
	return nil
}

func deleteOldData(db *pgx.Conn) error {
	ticker := time.NewTicker(5 * time.Minute)

	for range ticker.C {
		// Execute the SQL statement
		_, err := db.Exec(context.Background(), `
		TRUNCATE TABLE child_2;
		BEGIN;
		ALTER TABLE child_1 RENAME TO child_tmp;
		ALTER TABLE child_2 RENAME TO child_1;
		ALTER TABLE child_tmp RENAME TO child_2;
		COMMIT;
		`)
		if err != nil {
			panic(err)
		}

		fmt.Println("Table renamed successfully!")
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
	databaseUrl := "postgres://test:a63Nd2i5KCm@195.201.130.247:5432/mydb"
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseUrl)
	if err != nil {
		panic("failed to connect database")
	}
	fmt.Println("Database connected")

	// Chech if the table measurements_insert_1 exists
	// Check if the table exists
	var exists bool
	err = conn.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", "measurements_insert_1").Scan(&exists)
	if err != nil {
		panic(err)
	}
	if !exists {
		panic("Table measurements_insert_1 does not exist")
	}

	globalDb = conn

	fmt.Println("Starting go routine")
	go func() {
		err := deleteOldData(conn)
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
	// Generate a random client ID
	client := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port)).
		SetClientID("go-simple2").
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

	//--------------------------------------------------------------
	//							Shutdown
	//--------------------------------------------------------------

	for flag == false {
		time.Sleep(1 * time.Second)
	}

	if token := client.Unsubscribe("dev/#"); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	close(ch)

	err = conn.Close(ctx)
	if err != nil {
		return
	}
	//client.Disconnect(250)
}
