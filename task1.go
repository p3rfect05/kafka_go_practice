package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var errorLog = log.New(os.Stdout, "[ERROR]\t", log.Lshortfile|log.Ltime)
var infoLog = log.New(os.Stdout, "[INFO]\t", log.Ltime)
var connectRetries = 5

func task1() {
	doneChan := make(chan struct{})
	wg := sync.WaitGroup{}
	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
	if err != nil {

		for i := 0; i < connectRetries; i++ {
			time.Sleep(time.Second * 3)
			producer, err = sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
			if err == nil {
				break
			}
			errorLog.Printf("cannot create producer(probably connection error), retry %d/%d\n", i+1, connectRetries)

		}
		if err != nil {
			errorLog.Fatalln("could not create producer (connection error unresolved), shutting down...")
		}

	}
	received_msg := 0
	if err != nil {
		errorLog.Fatalln(err)
	}
	infoLog.Println("producer created")
	defer func() {
		if err := producer.Close(); err != nil {
			errorLog.Fatalln(err)
		}
	}()

	consumer, err := sarama.NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		errorLog.Fatalln(err)
	}
	infoLog.Println("consumer created")
	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetNewest)
	if err != nil {
		errorLog.Fatalln(err)
	}
	infoLog.Println("consumer registered at partition 0")
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			errorLog.Fatalln(err)
		}
	}()
	wg.Add(1)
	go func() {
		msgs := []string{"0th", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"}
		for _, msg_text := range msgs {
			msg := &sarama.ProducerMessage{
				Topic: "test-topic",
				Value: sarama.StringEncoder(msg_text),
			}
			partition, _, err := producer.SendMessage(msg)
			time.Sleep(time.Second)
			if err != nil {
				errorLog.Fatalf("%v at partition #%d\n", err, partition)
			}
		}
		for received_msg != len(msgs) {

		}
		doneChan <- struct{}{}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("message received at topic:%s, value:%s", msg.Topic, msg.Value)
				received_msg++

			case <-doneChan:
				log.Printf("%d messages received, ending\n", received_msg)
				return
			}
		}
	}()
	wg.Wait()
}
