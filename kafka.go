package main

import (
	"github.com/Shopify/sarama"
	"log"
)

type TopicMap struct {
	Msgs         chan *sarama.ConsumerMessage
	PartConsumer []sarama.PartitionConsumer
}

type KafkaClient struct {
	Consumer sarama.Consumer
	Topicmap map[string]*TopicMap
}

func NewKafkaClient() *KafkaClient {
	return &KafkaClient{Topicmap: make(map[string]*TopicMap)}
}

func (c *KafkaClient) NewConsumer(hostports []string) error {
	consumer, err := sarama.NewConsumer(hostports, nil)
	if err != nil {
		log.Printf("[kafka] new a consumer %+v error, %s\n", hostports, err)
	} else {
		log.Printf("[kafka] new a consumer %+v success.\n", hostports)
	}
	c.Consumer = consumer
	return err
}

func (c *KafkaClient) Close() {
	c.Consumer.Close()
	for _, v := range c.Topicmap {
		for i, _ := range v.PartConsumer {
			v.PartConsumer[i].Close()
		}
	}
	log.Printf("[kafka] close a connect success.\n")
}

func (c *KafkaClient) GetTopicMsg(topic string) {
	partitions, err := c.Consumer.Partitions(topic)
	log.Printf("[kafka] topic:%s, partitions:%+v\n", topic, partitions)
	if err == nil {
		if _, ok := c.Topicmap[topic]; ok == false {
			topic_m := &TopicMap{Msgs: make(chan *sarama.ConsumerMessage)}
			c.Topicmap[topic] = topic_m
		}
		for _, partition := range partitions {
			//每一个topic_partition创建一个go
			go c.OpenParttionConsumer(topic, partition)
		}
	}
}

func (c *KafkaClient) OpenParttionConsumer(topic string, partition int32) {
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err == nil {
		log.Printf("[kafka] topic[%s] at partition[%d] open a partitionconsumer\n", topic, partition)
		c.Topicmap[topic].PartConsumer = append(c.Topicmap[topic].PartConsumer, partitionConsumer)
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				c.Topicmap[topic].Msgs <- msg
			}
		}
	}
}
