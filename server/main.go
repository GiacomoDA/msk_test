package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var (
	producer    sarama.SyncProducer
	consumer    sarama.Consumer
	clientMap   = make(map[string]chan string)
	clientMapMu sync.Mutex
)

const (
	// The topic where responses are published
	replyTopic = "reply_topic"
)


func requestHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the query parameters
	topic := r.URL.Query().Get("topic")
	value := r.URL.Query().Get("value")
	requestID := r.URL.Query().Get("request_id")

	fmt.Println("Received HTTP request with values:", topic, " ", value, " ", requestID)

	// If any parameter is missing, send a bad request status
	if topic == "" || value == "" || requestID == "" {
		http.Error(w, "Missing topic, value, or request_id parameter", http.StatusBadRequest)
		return
	}

	// Create a channel to receive the response
	responseChan := make(chan string)
	clientMapMu.Lock()
	clientMap[requestID] = responseChan
	clientMapMu.Unlock()

	fmt.Println("Created channel to receive response")
	//fmt.Println(clientMap)

	// Publish the value to the given Kafka topic
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
		Key: sarama.StringEncoder(requestID),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message to Kafka: %v", err), http.StatusInternalServerError)
		clientMapMu.Lock()
		delete(clientMap, requestID)
		clientMapMu.Unlock()
		return
	}

	fmt.Println("Kafka event published")

	// Wait for the response or timeout
	select {
	case response := <-responseChan:
		fmt.Fprintf(w, "Response: %s", response)
	case <-time.After(10 * time.Second):
		http.Error(w, "Timeout waiting for response", http.StatusGatewayTimeout)
	}

	// Clean up the client map
	clientMapMu.Lock()
	delete(clientMap, requestID)
	clientMapMu.Unlock()
}

func consumeResponses() {
	// Make the consumer listen on the reply topic
	partitionConsumer, err := consumer.ConsumePartition(replyTopic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("Failed to start Kafka consumer:", err)
		return
	}
	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {

		// Assuming the message key is the request ID
		requestID := string(message.Key)
		response := string(message.Value)

		// use this with lambda function written in python to remove double quotations
		requestID = requestID[1:len(requestID)-1]
		response = response[1:len(response)-1]

		fmt.Println("Received reply: value=", response, " key=", requestID)

		clientMapMu.Lock()
		if responseChan, ok := clientMap[requestID]; ok {
			//fmt.Println("channel found")
			responseChan <- response
		}
		clientMapMu.Unlock()
	}
}

func main() {
	// Kafka broker addresses
	brokers := []string{
	//	"b-1.clustertest.crn5jl.c3.kafka.eu-north-1.amazonaws.com:9092",
	//	"b-2.clustertest.crn5jl.c3.kafka.eu-north-1.amazonaws.com:9092",
		"localhost:9092",
	}

	// Configure and start the Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Println("Failed to start Kafka producer:", err)
		return
	}
	defer producer.Close()

	// Start the Kafka consumer
	consumer, err = sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Failed to start Kafka consumer:", err)
		return
	}
	defer consumer.Close()

	// Start consuming responses in a separate goroutine
	go consumeResponses()

	// Register the requestHandler to handle requests at the root URL
	http.HandleFunc("/", requestHandler)

	// Start the server on port 8080
	fmt.Println("Server is listening on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Failed to start server:", err)
	}
}
