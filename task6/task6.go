package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var errorLog = log.New(os.Stdout, "[ERROR]\t", log.Lshortfile|log.Ltime)
var infoLog = log.New(os.Stdout, "[INFO]\t", log.Ltime)
var connectRetries = 5

func main() {
	wg := sync.WaitGroup{}
	producerErrors := 0

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

	}(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd := exec.Command("./task6_script.sh")
		cmd.Start()

	}()
	time.Sleep(time.Second)
	wg.Wait()
}
