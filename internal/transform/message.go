package transform

import "time"

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

func (m *Message) HeaderStringMap() map[string]string {
	result := make(map[string]string, len(m.Headers))
	for _, header := range m.Headers {
		result[header.Key] = string(header.Value)
	}
	return result
}

func (m *Message) SetHeader(key string, value []byte) {
	for idx := range m.Headers {
		if m.Headers[idx].Key == key {
			m.Headers[idx].Value = cloneBytes(value)
			return
		}
	}
	m.Headers = append(m.Headers, Header{
		Key:   key,
		Value: cloneBytes(value),
	})
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
