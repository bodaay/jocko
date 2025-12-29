package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/bodaay/quafka/log"
	"github.com/bodaay/quafka/protocol"
	jocko "github.com/bodaay/quafka/quafka"
	"github.com/bodaay/quafka/quafka/config"
)

type check struct {
	partition int32
	offset    int64
	message   string
}

const (
	topic         = "test_topic"
	messageCount  = 15
	clientID      = "test_client"
	numPartitions = int32(8)
)

var (
	logDir string
)

func init() {
	var err error
	logDir, err = os.MkdirTemp("/tmp", "jocko-client-test")
	if err != nil {
		panic(err)
	}
}

func main() {
	s, tmpDir := setup()
	defer func() {
		s.Shutdown()
		os.RemoveAll(tmpDir)
	}()

	cfg := sarama.NewConfig()
	cfg.ChannelBufferSize = 1
	cfg.Version = sarama.V0_10_0_1
	cfg.Producer.Return.Successes = true

	brokers := []string{s.Addr().String()}
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		panic(err)
	}

	pmap := make(map[int32][]check)

	for i := 0; i < messageCount; i++ {
		message := fmt.Sprintf("Hello from Jocko #%d!", i)
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			panic(err)
		}
		pmap[partition] = append(pmap[partition], check{
			partition: partition,
			offset:    offset,
			message:   message,
		})
	}
	if err = producer.Close(); err != nil {
		panic(err)
	}

	var totalChecked int
	for partitionID := range pmap {
		checked := 0
		consumer, err := sarama.NewConsumer(brokers, cfg)
		if err != nil {
			panic(err)
		}
		partition, err := consumer.ConsumePartition(topic, partitionID, 0)
		if err != nil {
			panic(err)
		}
		i := 0
		for msg := range partition.Messages() {
			fmt.Printf("msg partition [%d] offset [%d]\n", msg.Partition, msg.Offset)
			check := pmap[partitionID][i]
			if string(msg.Value) != check.message {
				log.Fatal.Printf("msg values not equal: partition: %d: offset: %d", msg.Partition, msg.Offset)
			}
			if msg.Offset != check.offset {
				log.Fatal.Printf("msg offsets not equal: partition: %d: offset: %d", msg.Partition, msg.Offset)
			}
			log.Info.Printf("msg is ok: partition: %d: offset: %d", msg.Partition, msg.Offset)
			i++
			checked++
			fmt.Printf("i: %d, len: %d\n", i, len(pmap[partitionID]))
			if i == len(pmap[partitionID]) {
				totalChecked += checked
				fmt.Println("checked partition:", partitionID)
				if err = consumer.Close(); err != nil {
					panic(err)
				}
				break
			} else {
				fmt.Println("still checking partition:", partitionID)
			}
		}
	}
	fmt.Printf("producer and consumer worked! %d messages ok\n", totalChecked)
}

func setup() (*jocko.Server, string) {
	s, tmpDir := jocko.NewTestServer(&testing.T{}, func(cfg *config.Config) {
		cfg.Bootstrap = true
		cfg.BootstrapExpect = 1
	}, nil)
	if err := s.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start cluster: %v\n", err)
		os.Exit(1)
	}
	// Wait for leader election before making connections
	time.Sleep(500 * time.Millisecond)

	conn, err := jocko.Dial("tcp", s.Addr().String())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error connecting to broker: %v\n", err)
		os.Exit(1)
	}
	resp, err := conn.CreateTopics(&protocol.CreateTopicRequests{
		Requests: []*protocol.CreateTopicRequest{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed with request to broker: %v\n", err)
		os.Exit(1)
	}
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() && topicErrCode.ErrorCode != protocol.ErrTopicAlreadyExists.Code() {
			err := protocol.Errs[topicErrCode.ErrorCode]
			fmt.Fprintf(os.Stderr, "error code: %v\n", err)
			os.Exit(1)
		}
	}

	return s, tmpDir
}
