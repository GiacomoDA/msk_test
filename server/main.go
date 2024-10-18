package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
	"log"

	"github.com/IBM/sarama"
)

var (
	producer     	sarama.SyncProducer
	consumer     	sarama.Consumer
	clientMap    	= make(map[string]chan string)
	requestTimes 	= make(map[string]time.Time)
	clientMapMu  	sync.Mutex
	requestTimesMu	sync.Mutex
	fileMu       	sync.Mutex
	logger		 	= log.New(os.Stdout, "", log.Ltime)
)

const (
	replyTopic = "reply_topic"
)

func requestHandler(w http.ResponseWriter, r *http.Request) {
	// Extract the query parameters
	topic := r.URL.Query().Get("topic")
	value := r.URL.Query().Get("value")
	requestID := r.URL.Query().Get("request_id")

	logger.Println("Received HTTP request:", topic, " ", value, " ", requestID)

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

	// Record the time when the request is published
	requestTimesMu.Lock()
	requestTimes[requestID] = time.Now()
	requestTimesMu.Unlock()

	// Publish the value to the given Kafka topic
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
		Key:   sarama.StringEncoder(requestID),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message to Kafka: %v", err), http.StatusInternalServerError)
		clientMapMu.Lock()
		delete(clientMap, requestID)
		clientMapMu.Unlock()
		return
	}

	//logger.Println("Kafka event published on topic ", topic)

	// Wait for the response or timeout
	select {
	case response := <-responseChan:
		// Calculate the time taken for the response to arrive
		requestTimesMu.Lock()
		startTime, exists := requestTimes[requestID]
		if exists {
			elapsedTime := time.Since(startTime)
			logger.Printf("Time taken for request %s: %v\n", requestID, elapsedTime)

			// Save latency to the file
			saveLatencyToFile(requestID, elapsedTime)
		}
		requestTimesMu.Unlock()

		fmt.Fprintf(w, "Response: %s\n", response)
	case <-time.After(10 * time.Second):
		http.Error(w, "Timeout waiting for response", http.StatusGatewayTimeout)
	}

	// Clean up the client map and request times map
	clientMapMu.Lock()
	delete(clientMap, requestID)
	clientMapMu.Unlock()

	requestTimesMu.Lock()
	delete(requestTimes, requestID)
	requestTimesMu.Unlock()
}

func consumeResponses() {
	// Get all partitions for the reply_topic
	partitions, err := consumer.Partitions(replyTopic)
	if err != nil {
		logger.Println("Failed to get partitions:", err)
		return
	}

	var wg sync.WaitGroup

	// Start a consumer for each partition
	for _, partition := range partitions {
		wg.Add(1)

		go func(partition int32) {
			defer wg.Done()

			// Create a consumer for the partition
			partitionConsumer, err := consumer.ConsumePartition(replyTopic, partition, sarama.OffsetNewest)
			if err != nil {
				logger.Printf("Failed to start consumer for partition %d: %v\n", partition, err)
				return
			}
			defer partitionConsumer.Close()

			// Consume messages from this partition
			for message := range partitionConsumer.Messages() {

				// Assuming the message key is the request ID
				requestID := string(message.Key)
				response := string(message.Value)

				logger.Printf("Received reply from partition %d: value=%s, key=%s\n", partition, response, requestID)

				clientMapMu.Lock()
				if responseChan, ok := clientMap[requestID]; ok {
					responseChan <- response
				}
				clientMapMu.Unlock()
			}
		}(partition)
	}

	// Wait for all partition consumers to finish
	wg.Wait()
}


func saveLatencyToFile(requestID string, latency time.Duration) {
	// Open the file in append mode
	fileMu.Lock()
	defer fileMu.Unlock()

	file, err := os.OpenFile("latency.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Println("Failed to open latency.txt:", err)
		return
	}
	defer file.Close()

	// Write the latency information to the file
	_, err = file.WriteString(fmt.Sprintf("%d\n", latency.Milliseconds()))
	if err != nil {
		logger.Println("Failed to write to latency.txt:", err)
	}
}

func main() {
	// Kafka broker addresses
	brokers := []string{
		"b-1.msktest.y2ths2.c3.kafka.eu-central-1.amazonaws.com:9092",
		"b-2.msktest.y2ths2.c3.kafka.eu-central-1.amazonaws.com:9092",
		//"localhost:9092",
	}

	// Configure and start the Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		logger.Println("Failed to start Kafka producer:", err)
		return
	}
	defer producer.Close()

	// Start the Kafka consumer
	consumer, err = sarama.NewConsumer(brokers, nil)
	if err != nil {
		logger.Println("Failed to start Kafka consumer:", err)
		return
	}
	defer consumer.Close()

	// Start consuming responses in a separate goroutine
	go consumeResponses()

	// Register the requestHandler to handle requests at the root URL
	http.HandleFunc("/", requestHandler)

	// Start the server on port 8080
	logger.Println("Server is listening on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.Println("Failed to start server:", err)
	}
}
