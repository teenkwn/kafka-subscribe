package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
)

type (
	consumerGroup struct {
		ctx     context.Context
		topic   Topic
		groupId string
		group   sarama.ConsumerGroup
		handler *consumerGroupHandler
		closed  bool
	}

	consumerGroupHandler struct {
		groupId string
		claimer func([]byte)
	}
)

func main() {
	// Hello world, the web server

	helloHandler := func(w http.ResponseWriter, req *http.Request) {
		fmt.Println("hello test world1")
		io.WriteString(w, "Hello, world!\n")
		fmt.Println("hello test world")
	}

	http.HandleFunc("/", helloHandler)
	log.Println("Listing for requests at http://localhost:8000/")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func NewConsumer(
	ctx context.Context,
	brokers []string,
	groupId string,
	topic Topic,
) (*consumerGroup, error) {

	config := buildKafkaSubscribeConfig()
	group, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Error("creating consumer group: ", err)
		return nil, err
	}

	return &consumerGroup{
		ctx:     ctx,
		topic:   topic,
		groupId: groupId,
		group:   group,
		handler: &consumerGroupHandler{},
		closed:  false,
	}, nil
}
