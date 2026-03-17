package service

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestMessageRecordRoundTrip(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Topic:     "source-topic",
		Key:       []byte{0x01, 0x02},
		Value:     []byte{0xFF, 0x00, 0x7F},
		Timestamp: time.Unix(100, 0).UTC(),
		Headers: []*sarama.RecordHeader{
			{
				Key:   []byte("trace-id"),
				Value: []byte{0x0A, 0x0B},
			},
		},
	}

	record := ConsumerMessageToRecord(msg)
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded MessageRecord
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	producerMsg, err := decoded.ToProducerMessage("target-topic")
	if err != nil {
		t.Fatalf("ToProducerMessage() error = %v", err)
	}

	gotValue, err := producerMsg.Value.Encode()
	if err != nil {
		t.Fatalf("encode producer value error = %v", err)
	}
	if !bytes.Equal(gotValue, msg.Value) {
		t.Fatalf("value mismatch: got=%v want=%v", gotValue, msg.Value)
	}

	gotKey, err := producerMsg.Key.Encode()
	if err != nil {
		t.Fatalf("encode producer key error = %v", err)
	}
	if !bytes.Equal(gotKey, msg.Key) {
		t.Fatalf("key mismatch: got=%v want=%v", gotKey, msg.Key)
	}

	if len(producerMsg.Headers) != 1 {
		t.Fatalf("headers count = %d, want 1", len(producerMsg.Headers))
	}
	if string(producerMsg.Headers[0].Key) != "trace-id" {
		t.Fatalf("header key mismatch: got=%s", string(producerMsg.Headers[0].Key))
	}
	if !bytes.Equal(producerMsg.Headers[0].Value, []byte{0x0A, 0x0B}) {
		t.Fatalf("header value mismatch: got=%v", producerMsg.Headers[0].Value)
	}
}

func TestRecordToTransformMessageAndProducerMessage(t *testing.T) {
	record := MessageRecord{
		Topic: "source-topic",
		Key:   "a2V5",
		Value: "eyJpZCI6MX0=",
		Headers: []RecordHeader{
			{
				Key:   "trace-id",
				Value: "MTIz",
			},
		},
		Timestamp: time.Unix(200, 0).UTC(),
	}

	transformMessage, err := record.ToTransformMessage()
	if err != nil {
		t.Fatalf("ToTransformMessage() error = %v", err)
	}
	if string(transformMessage.Key) != "key" {
		t.Fatalf("transform key = %q, want key", string(transformMessage.Key))
	}
	if string(transformMessage.Value) != `{"id":1}` {
		t.Fatalf("transform value = %q, want json payload", string(transformMessage.Value))
	}

	producerMessage, err := TransformMessageToProducerMessage(transformMessage, "target-topic")
	if err != nil {
		t.Fatalf("TransformMessageToProducerMessage() error = %v", err)
	}
	encodedValue, err := producerMessage.Value.Encode()
	if err != nil {
		t.Fatalf("encode producer value error = %v", err)
	}
	if string(encodedValue) != `{"id":1}` {
		t.Fatalf("producer value = %q, want json payload", string(encodedValue))
	}
	if len(producerMessage.Headers) != 1 || string(producerMessage.Headers[0].Value) != "123" {
		t.Fatalf("producer headers mismatch: %+v", producerMessage.Headers)
	}
}
