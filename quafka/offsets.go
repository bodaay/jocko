package jocko

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/bodaay/quafka/protocol"
)

// OffsetCommitKey represents the key for an offset commit message
type OffsetCommitKey struct {
	GroupID   string
	Topic     string
	Partition int32
}

// Encode encodes the offset commit key
func (k *OffsetCommitKey) Encode() []byte {
	buf := new(bytes.Buffer)
	// Group ID
	binary.Write(buf, binary.BigEndian, int16(len(k.GroupID)))
	buf.WriteString(k.GroupID)
	// Topic
	binary.Write(buf, binary.BigEndian, int16(len(k.Topic)))
	buf.WriteString(k.Topic)
	// Partition
	binary.Write(buf, binary.BigEndian, k.Partition)
	return buf.Bytes()
}

// DecodeOffsetCommitKey decodes an offset commit key
func DecodeOffsetCommitKey(data []byte) (*OffsetCommitKey, error) {
	key := &OffsetCommitKey{}
	buf := bytes.NewReader(data)

	// Group ID
	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupID := make([]byte, groupIDLen)
	if _, err := buf.Read(groupID); err != nil {
		return nil, err
	}
	key.GroupID = string(groupID)

	// Topic
	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topic := make([]byte, topicLen)
	if _, err := buf.Read(topic); err != nil {
		return nil, err
	}
	key.Topic = string(topic)

	// Partition
	if err := binary.Read(buf, binary.BigEndian, &key.Partition); err != nil {
		return nil, err
	}

	return key, nil
}

// OffsetCommitValue represents the value for an offset commit message
type OffsetCommitValue struct {
	Offset    int64
	Metadata  *string
	Timestamp int64
}

// Encode encodes the offset commit value
func (v *OffsetCommitValue) Encode() []byte {
	buf := new(bytes.Buffer)
	// Offset
	binary.Write(buf, binary.BigEndian, v.Offset)
	// Metadata (nullable string)
	if v.Metadata == nil {
		binary.Write(buf, binary.BigEndian, int16(-1))
	} else {
		binary.Write(buf, binary.BigEndian, int16(len(*v.Metadata)))
		buf.WriteString(*v.Metadata)
	}
	// Timestamp
	binary.Write(buf, binary.BigEndian, v.Timestamp)
	return buf.Bytes()
}

// DecodeOffsetCommitValue decodes an offset commit value
func DecodeOffsetCommitValue(data []byte) (*OffsetCommitValue, error) {
	value := &OffsetCommitValue{}
	buf := bytes.NewReader(data)

	// Offset
	if err := binary.Read(buf, binary.BigEndian, &value.Offset); err != nil {
		return nil, err
	}

	// Metadata (nullable string)
	var metadataLen int16
	if err := binary.Read(buf, binary.BigEndian, &metadataLen); err != nil {
		return nil, err
	}
	if metadataLen >= 0 {
		metadata := make([]byte, metadataLen)
		if _, err := buf.Read(metadata); err != nil {
			return nil, err
		}
		metaStr := string(metadata)
		value.Metadata = &metaStr
	}

	// Timestamp
	if err := binary.Read(buf, binary.BigEndian, &value.Timestamp); err != nil {
		return nil, err
	}

	return value, nil
}

// EncodeOffsetCommitMessage encodes an offset commit as a Kafka message
func EncodeOffsetCommitMessage(groupID, topic string, partition int32, offset int64, metadata *string, timestamp int64) ([]byte, error) {
	key := &OffsetCommitKey{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
	}
	value := &OffsetCommitValue{
		Offset:    offset,
		Metadata:  metadata,
		Timestamp: timestamp,
	}

	keyBytes := key.Encode()
	valueBytes := value.Encode()

	// Create a message with magic byte 1 (has timestamp)
	msg := &protocol.Message{
		MagicByte: 1,
		Timestamp: time.Unix(timestamp/1000, (timestamp%1000)*int64(time.Millisecond)),
		Key:       keyBytes,
		Value:     valueBytes,
	}

	// Create a message set with the message
	msgSet := &protocol.MessageSet{
		Offset:   0, // Will be set by commit log
		Messages: []*protocol.Message{msg},
	}

	// Encode the message set to bytes
	return protocol.Encode(msgSet)
}

// DecodeOffsetCommitMessage decodes an offset commit message from the offsets topic
func DecodeOffsetCommitMessage(data []byte) (*OffsetCommitKey, *OffsetCommitValue, error) {
	decoder := protocol.NewDecoder(data)
	msgSet := &protocol.MessageSet{}
	if err := msgSet.Decode(decoder); err != nil {
		return nil, nil, err
	}

	if len(msgSet.Messages) == 0 {
		return nil, nil, nil
	}

	msg := msgSet.Messages[0]
	return DecodeOffsetCommitMessageFromProtocol(msg)
}

// DecodeOffsetCommitMessageFromProtocol decodes an offset commit from a protocol Message
func DecodeOffsetCommitMessageFromProtocol(msg *protocol.Message) (*OffsetCommitKey, *OffsetCommitValue, error) {
	key, err := DecodeOffsetCommitKey(msg.Key)
	if err != nil {
		return nil, nil, err
	}

	value, err := DecodeOffsetCommitValue(msg.Value)
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// ReadOffsetCommitMessages reads offset commit messages from a commit log reader
func ReadOffsetCommitMessages(reader io.Reader) (map[string]map[int32]int64, error) {
	// Map: topic -> partition -> offset
	offsets := make(map[string]map[int32]int64)
	for {
		// Read offset (8 bytes) and size (4 bytes)
		header := make([]byte, 12)
		n, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if n < 12 {
			break
		}

		size := int32(binary.BigEndian.Uint32(header[8:12]))
		if size <= 0 {
			continue
		}

		// Read the message set payload
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Decode the message set
		msgSetData := append(header, payload...)
		key, value, err := DecodeOffsetCommitMessage(msgSetData)
		if err != nil {
			// Skip invalid messages
			continue
		}
		if key == nil || value == nil {
			continue
		}

		// Store the offset
		if offsets[key.Topic] == nil {
			offsets[key.Topic] = make(map[int32]int64)
		}
		// Keep the latest offset for each topic-partition
		if currentOffset, exists := offsets[key.Topic][key.Partition]; !exists || value.Offset > currentOffset {
			offsets[key.Topic][key.Partition] = value.Offset
		}
	}

	return offsets, nil
}

