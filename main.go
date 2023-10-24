package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	viper "github.com/spf13/viper"
	"log"
)

func main() {
	viper.SetConfigFile("config/config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Error reading config file: %s\n", err)
		viper.SetConfigFile("config.yaml")
		err = viper.ReadInConfig()
		if err != nil {
			fmt.Printf("Error reading config file: %s\n", err)
			return
		}
	}

	kafkaUserName := viper.GetString("username")
	kafkaPassWord := viper.GetString("pw")
	kafkaBootStrap := viper.GetString("bs")
	go producer(kafkaUserName, kafkaPassWord, kafkaBootStrap)
	consumer(kafkaUserName, kafkaPassWord, kafkaBootStrap)

}

func producer(kafkaUserName, kafkaPassWord, kafkaBootStrap string) {
	mechanism, err := scram.Mechanism(scram.SHA256, kafkaUserName, kafkaPassWord)
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}
	conn, err := dialer.DialLeader(context.Background(), "tcp", kafkaBootStrap, "test", 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	//	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	var msg = ""
	for {
		log.Println("type message")
		_, _ = fmt.Scan(&msg)
		_, err = conn.WriteMessages(

			kafka.Message{Value: []byte(msg)},
		)
		if err == nil {
			log.Println("send message")
		}
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

	}

}

func consumer(kafkaUserName, kafkaPassWord, kafkaBootStrap string) {
	mechanism, err := scram.Mechanism(scram.SHA256, kafkaUserName, kafkaPassWord)
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBootStrap},
		Topic:     "test",
		Partition: 0,
		MaxBytes:  10e5, // 10MB
		Dialer:    dialer,
	})
	//r.SetOffset(1)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
