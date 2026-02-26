// Package kafka implements the Kafka source and sink for Mako.
//
// Uses franz-go (pure Go, zero CGO) for maximum portability.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// Source reads events from a Kafka topic.
type Source struct {
	brokers       []string
	topic         string
	consumerGroup string
	startOffset   string

	eventCh chan *pipeline.Event
	lag     atomic.Int64
	closed  atomic.Bool
}

// NewSource creates a Kafka source.
func NewSource(brokers, topic, consumerGroup, startOffset string) *Source {
	return &Source{
		brokers:       strings.Split(brokers, ","),
		topic:         topic,
		consumerGroup: consumerGroup,
		startOffset:   startOffset,
		eventCh:       make(chan *pipeline.Event, 1000),
	}
}

// Open connects to Kafka. In a real implementation, this initializes
// a franz-go consumer. For the framework skeleton, we define the interface.
func (s *Source) Open(ctx context.Context) error {
	// In production: initialize franz-go consumer
	// kgo.NewClient(
	//     kgo.SeedBrokers(s.brokers...),
	//     kgo.ConsumerGroup(s.consumerGroup),
	//     kgo.ConsumeTopics(s.topic),
	// )
	return nil
}

// Read returns the event channel.
func (s *Source) Read(ctx context.Context) (<-chan *pipeline.Event, error) {
	return s.eventCh, nil
}

// Close shuts down the consumer.
func (s *Source) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.eventCh)
	}
	return nil
}

// Lag returns the current consumer lag.
func (s *Source) Lag() int64 {
	return s.lag.Load()
}

// ═══════════════════════════════════════════
// Kafka Sink
// ═══════════════════════════════════════════

// Sink writes events to a Kafka topic.
type Sink struct {
	brokers []string
	topic   string
}

// NewSink creates a Kafka sink.
func NewSink(brokers, topic string) *Sink {
	return &Sink{
		brokers: strings.Split(brokers, ","),
		topic:   topic,
	}
}

func (s *Sink) Open(ctx context.Context) error   { return nil }
func (s *Sink) Name() string                      { return "kafka:" + s.topic }
func (s *Sink) Flush(ctx context.Context) error   { return nil }
func (s *Sink) Close() error                      { return nil }

func (s *Sink) Write(ctx context.Context, events []*pipeline.Event) error {
	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		_ = data // In production: produce to kafka via franz-go
	}
	return nil
}
