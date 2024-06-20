package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	ready chan bool
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}
func task4() {
	doneChan := make(chan struct{})
	wg := sync.WaitGroup{}
	successes := 0
	producerErrors := 0
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
	infoLog.Println("producer created")
	defer func() {
		if err := producer.Close(); err != nil {
			errorLog.Fatalln(err)
		}
	}()

	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer := Consumer{
		ready: make(chan bool),
	}

	client, err := sarama.NewConsumerGroup([]string{"kafka:9092"}, "group1", config)
	if err != nil {
		errorLog.Fatalln(err)
	}
	ctx := context.Background()
	for {
		if err := client.Consume(ctx, []string{"test-topic"}, &consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
		}
	}

}
