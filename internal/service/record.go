package service

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"kafka-redirect/internal/transform"
)

type RecordHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MessageRecord struct {
	Topic     string         `json:"topic"`
	Key       string         `json:"key,omitempty"`
	Value     string         `json:"value"`
	Headers   []RecordHeader `json:"headers,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

func ConsumerMessageToRecord(msg *sarama.ConsumerMessage) MessageRecord {
	record := MessageRecord{
		Topic:     msg.Topic,
		Value:     base64.StdEncoding.EncodeToString(msg.Value),
		Timestamp: msg.Timestamp.UTC(),
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	if len(msg.Key) > 0 {
		record.Key = base64.StdEncoding.EncodeToString(msg.Key)
	}

	if len(msg.Headers) > 0 {
		record.Headers = make([]RecordHeader, 0, len(msg.Headers))
		for _, header := range msg.Headers {
			if header == nil {
				continue
			}
			record.Headers = append(record.Headers, RecordHeader{
				Key:   string(header.Key),
				Value: base64.StdEncoding.EncodeToString(header.Value),
			})
		}
	}
	return record
}

func ConsumerMessageToTransformMessage(msg *sarama.ConsumerMessage) *transform.Message {
	if msg == nil {
		return nil
	}

	result := &transform.Message{
		Key:       cloneBytes(msg.Key),
		Value:     cloneBytes(msg.Value),
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}
	if len(msg.Headers) > 0 {
		result.Headers = make([]transform.Header, 0, len(msg.Headers))
		for _, header := range msg.Headers {
			if header == nil {
				continue
			}
			result.Headers = append(result.Headers, transform.Header{
				Key:   string(header.Key),
				Value: cloneBytes(header.Value),
			})
		}
	}
	return result
}

func (r MessageRecord) ToTransformMessage() (*transform.Message, error) {
	valueBytes, err := base64.StdEncoding.DecodeString(r.Value)
	if err != nil {
		return nil, fmt.Errorf("decode value base64: %w", err)
	}

	var keyBytes []byte
	if r.Key != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(r.Key)
		if err != nil {
			return nil, fmt.Errorf("decode key base64: %w", err)
		}
	}

	result := &transform.Message{
		Key:       cloneBytes(keyBytes),
		Value:     cloneBytes(valueBytes),
		Topic:     r.Topic,
		Timestamp: r.Timestamp,
	}

	if len(r.Headers) > 0 {
		result.Headers = make([]transform.Header, 0, len(r.Headers))
		for _, header := range r.Headers {
			rawHeaderValue, decodeErr := base64.StdEncoding.DecodeString(header.Value)
			if decodeErr != nil {
				return nil, fmt.Errorf("decode header value base64: %w", decodeErr)
			}
			result.Headers = append(result.Headers, transform.Header{
				Key:   header.Key,
				Value: cloneBytes(rawHeaderValue),
			})
		}
	}

	return result, nil
}

func TransformMessageToProducerMessage(
	msg *transform.Message,
	targetTopic string,
) (*sarama.ProducerMessage, error) {
	if msg == nil {
		return nil, errors.New("transform message is nil")
	}

	topic := strings.TrimSpace(targetTopic)
	if topic == "" {
		return nil, errors.New("target topic is empty")
	}

	result := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(cloneBytes(msg.Value)),
	}
	if len(msg.Key) > 0 {
		result.Key = sarama.ByteEncoder(cloneBytes(msg.Key))
	}
	if !msg.Timestamp.IsZero() {
		result.Timestamp = msg.Timestamp
	}

	if len(msg.Headers) > 0 {
		result.Headers = make([]sarama.RecordHeader, 0, len(msg.Headers))
		for _, header := range msg.Headers {
			result.Headers = append(result.Headers, sarama.RecordHeader{
				Key:   []byte(header.Key),
				Value: cloneBytes(header.Value),
			})
		}
	}

	return result, nil
}

func (r MessageRecord) ToProducerMessage(targetTopic string) (*sarama.ProducerMessage, error) {
	transformMessage, err := r.ToTransformMessage()
	if err != nil {
		return nil, err
	}
	return TransformMessageToProducerMessage(transformMessage, targetTopic)
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
