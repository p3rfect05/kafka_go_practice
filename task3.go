package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func task3() {

	doneChan := make(chan struct{})
	wg := sync.WaitGroup{}
	successes := 0
	producerErrors := 0
	received_msg := 0

	msgs := []string{"0th", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"}

	consumerWorkers := 5

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"kafka:9092"}, nil)
	if err != nil {

		for i := 0; i < connectRetries; i++ {
			time.Sleep(time.Second * 3)
			producer, err = sarama.NewAsyncProducer([]string{"kafka:9092"}, nil)
			if err == nil {
				break
			}
			errorLog.Printf("cannot not create producer(probably connection error), retry %d/%d\n", i+1, connectRetries)

		}
		if err != nil {
			errorLog.Fatalln("could not create producer (connection error unresolved), shutting down...")
		}

	}

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
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			producerErrors++
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go func(ctx context.Context) {
		for _, msg_text := range msgs {
			msg := &sarama.ProducerMessage{
				Topic: "test-topic",
				Value: sarama.StringEncoder(msg_text),
			}
			producer.Input() <- msg

		}
		// we either receive all messages (with or without errors) or will exit in 15 seconds
		for len(msgs) != received_msg+producerErrors {
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}

		}
		doneChan <- struct{}{}
	}(ctx)

	wg.Add(consumerWorkers)
	// consumer waiting for either a message or a done signal
	for i := 0; i < consumerWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					log.Printf("message received at topic:%s, value:%s", msg.Topic, msg.Value)
					received_msg++

				case <-doneChan:
					log.Printf("%d/%d messages received, ending\n", received_msg, len(msgs))
					producer.AsyncClose()
					return
				}
			}
		}()
	}
	wg.Wait()
}
