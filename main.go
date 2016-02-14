package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
)

var host = flag.String("h", "localhost:9092", "kafka hosts slice")
var topic = flag.String("t", "islog", "kafka topic")

func main() {
	flag.Parse()
	if *host == "" || *topic == "" {
		fmt.Printf("pararm error,host=%s,topic=%s\n", *host, *topic)
		os.Exit(0)
	}

	hosts := strings.Split(*host, ",")
	//kafka
	kafkaClient := NewKafkaClient()
	err := kafkaClient.NewConsumer(hosts)
	if err != nil {
		os.Exit(-2)
	}
	defer kafkaClient.Close()
	kafkaClient.GetTopicMsg(*topic)

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

ConsumerLoop:
	for {
		select {
		case msg := <-kafkaClient.Topicmap[*topic].Msgs:
			log.Println("get msg ", string(msg.Value))
		case <-signals:
			break ConsumerLoop
		}
	}

}
