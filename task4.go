package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func task4() {
	doneChan := make(chan struct{})
	wg := sync.WaitGroup{}
	producerErrors := 0
	config := sarama.NewConfig() // specify appropriate version
	config.Consumer.Return.Errors = true
	received_msg := 0

	msgs := []string{"0th", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"}
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
	defer func() {
		if err := producer.Close(); err != nil {
			errorLog.Fatalln(err)
		}
	}()
	infoLog.Println("producer created")

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
	group, err := sarama.NewConsumerGroup([]string{"kafka:9092"}, "my-group", config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx = context.Background()
	for {
		topics := []string{"my-topic"}
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
