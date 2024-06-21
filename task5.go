package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func task5() {
	doneChan := make(chan struct{})
	wg := sync.WaitGroup{}
	successes := 0
	producerErrors := 0
	received_msg := 0
	consumerWorkers := 3
	msgs := []string{"0th", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th"}
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{"kafka:9092"}, config)
	if err != nil {

		for i := 0; i < connectRetries; i++ {
			time.Sleep(time.Second * 3)
			admin, err = sarama.NewClusterAdmin([]string{"kafka:9092"}, config)
			if err == nil {
				break
			}
			errorLog.Printf("cannot not create cluster admin(probably connection error), retry %d/%d\n", i+1, connectRetries)

		}
		if err != nil {
			errorLog.Fatalln("could not create cluster admin (connection error unresolved), shutting down...")
		}

	}
	if err != nil {
		errorLog.Fatalln(err)
	}
	defer func() {
		admin.Close()
	}()

	err = admin.CreateTopic("test-topic", &sarama.TopicDetail{
		NumPartitions:     int32(consumerWorkers),
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		errorLog.Fatalln(err)
	}

	producer, err := sarama.NewAsyncProducer([]string{"kafka:9092"}, nil)
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

	var partitionConsumers []sarama.PartitionConsumer = make([]sarama.PartitionConsumer, consumerWorkers)
	for i := int32(0); i < int32(consumerWorkers); i++ {
		partitionConsumer, err := consumer.ConsumePartition("test-topic", i, sarama.OffsetNewest)
		if err != nil {
			errorLog.Fatalln(err)
		}
		partitionConsumers[i] = partitionConsumer
		infoLog.Println("consumer registered at partition ", i)
	}

	defer func() {
		for _, partitionConsumer := range partitionConsumers {
			if err := partitionConsumer.Close(); err != nil {
				errorLog.Fatalln(err)
			}
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
		for i, msg_text := range msgs {
			msg := &sarama.ProducerMessage{
				Topic:     "test-topic",
				Value:     sarama.StringEncoder(msg_text),
				Partition: int32(i % 2),
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
				case msg := <-partitionConsumers[i].Messages():
					log.Printf("message received by consumer #%d at topic:%s, value:%s", i, msg.Topic, msg.Value)
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
