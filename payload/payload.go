package payload

import (
	"encoding/json"
	"time"
)

type Payload struct {
	MqttTopic        string    `json:"mqtt_topic"`
	Message          []byte    `json:"message"`
	MessageTimestamp time.Time `json:"message_timestamp"`
}

// Serialize converts the Payload to a byte slice (e.g., JSON)
func (p *Payload) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

// Deserialize converts a byte slice to a Payload
func Deserialize(data []byte) (*Payload, error) {
	var p Payload
	err := json.Unmarshal(data, &p)
	return &p, err
}
