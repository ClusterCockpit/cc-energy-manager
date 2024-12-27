package ccmessage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	write "github.com/influxdata/influxdb-client-go/v2/api/write"
	lp1 "github.com/influxdata/line-protocol" // MIT license
	lp2 "github.com/influxdata/line-protocol/v2/lineprotocol"
	"golang.org/x/exp/maps"
)

type CCMessageType int

const (
	CCMSG_TYPE_METRIC = iota
	CCMSG_TYPE_EVENT
	CCMSG_TYPE_LOG
	CCMSG_TYPE_CONTROL
)
const MIN_CCMSG_TYPE = CCMSG_TYPE_METRIC
const MAX_CCMSG_TYPE = CCMSG_TYPE_CONTROL
const CCMSG_TYPE_INVALID = MAX_CCMSG_TYPE + 1

// Most functions are derived from github.com/influxdata/line-protocol/metric.go
// The metric type is extended with an extra meta information list re-using the Tag
// type.
//
// See: https://docs.influxdata.com/influxdb/latest/reference/syntax/line-protocol/
type ccMessage struct {
	name   string                 // Measurement name
	meta   map[string]string      // map of meta data tags
	tags   map[string]string      // map of of tags
	fields map[string]interface{} // map of of fields
	tm     time.Time              // timestamp
}

type ccMessageJSON struct {
	Name string `json:"name"` // Measurement name
	//Meta   map[string]string      `json:"meta,omitempty"` // map of meta data tags
	Tags   map[string]string      `json:"tags"`      // map of of tags
	Fields map[string]interface{} `json:"fields"`    // map of of fields
	Tm     time.Time              `json:"timestamp"` // timestamp
}

// ccMessage access functions
type CCMessage interface {
	ToPoint(metaAsTags map[string]bool) *write.Point  // Generate influxDB point for data type ccMessage
	ToLineProtocol(metaAsTags map[string]bool) string // Generate influxDB line protocol for data type ccMessage
	ToJSON(metaAsTags map[string]bool) (json.RawMessage, error)

	Name() string        // Get metric name
	SetName(name string) // Set metric name

	Time() time.Time     // Get timestamp
	SetTime(t time.Time) // Set timestamp

	Tags() map[string]string                   // Map of tags
	AddTag(key, value string)                  // Add a tag
	GetTag(key string) (value string, ok bool) // Get a tag by its key
	HasTag(key string) (ok bool)               // Check if a tag key is present
	RemoveTag(key string)                      // Remove a tag by its key

	Meta() map[string]string                    // Map of meta data tags
	AddMeta(key, value string)                  // Add a meta data tag
	GetMeta(key string) (value string, ok bool) // Get a meta data tab addressed by its key
	HasMeta(key string) (ok bool)               // Check if a meta data key is present
	RemoveMeta(key string)                      // Remove a meta data tag by its key

	Fields() map[string]interface{}                   // Map of fields
	AddField(key string, value interface{})           // Add a field
	GetField(key string) (value interface{}, ok bool) // Get a field addressed by its key
	HasField(key string) (ok bool)                    // Check if a field key is present
	RemoveField(key string)                           // Remove a field addressed by its key
	String() string                                   // Return line-protocol like string

	MessageType() CCMessageType // Return message type
	//Validate(hostnameTag string) bool // Validate that it is a valid CCMessage
}

// String implements the stringer interface for data type ccMessage
func (m *ccMessage) String() string {
	return fmt.Sprintf(
		"Name: %s, Tags: %+v, Meta: %+v, fields: %+v, Timestamp: %d",
		m.name, m.tags, m.meta, m.fields, m.tm.UnixNano(),
	)
}

// ToLineProtocol generates influxDB line protocol for data type ccMessage
func (m *ccMessage) ToPoint(metaAsTags map[string]bool) (p *write.Point) {
	p = influxdb2.NewPoint(m.name, m.tags, m.fields, m.tm)
	for key, use_as_tag := range metaAsTags {
		if use_as_tag {
			if value, ok := m.GetMeta(key); ok {
				p.AddTag(key, value)
			}
		}
	}
	return p
}

// ToLineProtocol generates influxDB line protocol for data type ccMessage
func (m *ccMessage) ToLineProtocol(metaAsTags map[string]bool) string {

	return write.PointToLineProtocol(
		m.ToPoint(metaAsTags),
		time.Nanosecond,
	)
}

func (m *ccMessage) ToJSON(metaAsTags map[string]bool) (json.RawMessage, error) {
	metalen := len(m.meta) - len(metaAsTags)
	if metalen < 0 {
		metalen = 0
	}
	mc := ccMessageJSON{
		Name: m.name,
		Tm:   m.tm,
		Tags: maps.Clone(m.tags),
		//Meta:   make(map[string]string, metalen),
		Fields: maps.Clone(m.fields),
	}
	for k := range metaAsTags {
		if v, ok := m.meta[k]; ok {
			mc.Tags[k] = v
		}
	}

	return json.Marshal(mc)
}

// Name returns the measurement name
func (m *ccMessage) Name() string {
	return m.name
}

// SetName sets the measurement name
func (m *ccMessage) SetName(name string) {
	m.name = name
}

// Time returns timestamp
func (m *ccMessage) Time() time.Time {
	return m.tm
}

// SetTime sets the timestamp
func (m *ccMessage) SetTime(t time.Time) {
	m.tm = t
}

// Tags returns the the list of tags as key-value-mapping
func (m *ccMessage) Tags() map[string]string {
	return m.tags
}

// AddTag adds a tag (consisting of key and value) to the map of tags
func (m *ccMessage) AddTag(key, value string) {
	m.tags[key] = value
}

// GetTag returns the tag with tag's key equal to <key>
func (m *ccMessage) GetTag(key string) (string, bool) {
	value, ok := m.tags[key]
	return value, ok
}

// HasTag checks if a tag with key equal to <key> is present in the list of tags
func (m *ccMessage) HasTag(key string) bool {
	_, ok := m.tags[key]
	return ok
}

// RemoveTag removes the tag with tag's key equal to <key>
func (m *ccMessage) RemoveTag(key string) {
	delete(m.tags, key)
}

// Meta returns the meta data tags as key-value mapping
func (m *ccMessage) Meta() map[string]string {
	return m.meta
}

// AddMeta adds a meta data tag (consisting of key and value) to the map of meta data tags
func (m *ccMessage) AddMeta(key, value string) {
	m.meta[key] = value
}

// GetMeta returns the meta data tag with meta data's key equal to <key>
func (m *ccMessage) GetMeta(key string) (string, bool) {
	value, ok := m.meta[key]
	return value, ok
}

// HasMeta checks if a meta data tag with meta data's key equal to <key> is present in the map of meta data tags
func (m *ccMessage) HasMeta(key string) bool {
	_, ok := m.meta[key]
	return ok
}

// RemoveMeta removes the meta data tag with tag's key equal to <key>
func (m *ccMessage) RemoveMeta(key string) {
	delete(m.meta, key)
}

// Fields returns the list of fields as key-value-mapping
func (m *ccMessage) Fields() map[string]interface{} {
	return m.fields
}

// AddField adds a field (consisting of key and value) to the map of fields
func (m *ccMessage) AddField(key string, value interface{}) {
	m.fields[key] = value
}

// GetField returns the field with field's key equal to <key>
func (m *ccMessage) GetField(key string) (interface{}, bool) {
	v, ok := m.fields[key]
	return v, ok
}

// HasField checks if a field with field's key equal to <key> is present in the map of fields
func (m *ccMessage) HasField(key string) bool {
	_, ok := m.fields[key]
	return ok
}

// RemoveField removes the field with field's key equal to <key>
// from the map of fields
func (m *ccMessage) RemoveField(key string) {
	delete(m.fields, key)
}

// New creates a new measurement point
func NewMessage(
	name string,
	tags map[string]string,
	meta map[string]string,
	fields map[string]interface{},
	tm time.Time,
) (CCMessage, error) {
	m := &ccMessage{
		name:   name,
		tags:   maps.Clone(tags),
		meta:   maps.Clone(meta),
		fields: make(map[string]interface{}, len(fields)),
		tm:     tm,
	}

	// deep copy fields
	for k, v := range fields {
		v := convertField(v)
		if v == nil {
			continue
		}
		m.fields[k] = v
	}

	return m, nil
}

// FromMetric copies the metric <other>
func FromMessage(other CCMessage) CCMessage {

	return &ccMessage{
		name:   other.Name(),
		tags:   maps.Clone(other.Tags()),
		meta:   maps.Clone(other.Meta()),
		fields: maps.Clone(other.Fields()),
		tm:     other.Time(),
	}
}

func EmptyMessage() CCMessage {
	return &ccMessage{
		name:   "",
		tags:   make(map[string]string),
		meta:   make(map[string]string),
		fields: make(map[string]interface{}),
		tm:     time.Time{},
	}
}

// FromInfluxMetric copies the influxDB line protocol metric <other>
func FromInfluxMetric(other lp1.Metric) CCMessage {
	m := &ccMessage{
		name:   other.Name(),
		tags:   make(map[string]string),
		meta:   make(map[string]string),
		fields: make(map[string]interface{}),
		tm:     other.Time(),
	}

	// deep copy tags and fields
	for _, otherTag := range other.TagList() {
		m.tags[otherTag.Key] = otherTag.Value
	}
	for _, otherField := range other.FieldList() {
		m.fields[otherField.Key] = otherField.Value
	}
	return m
}

func FromJSON(input json.RawMessage) (CCMessage, error) {
	var j ccMessageJSON
	err := json.Unmarshal(input, &j)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON to CCMessage: %v", err.Error())
	}

	return NewMessage(j.Name, j.Tags, make(map[string]string), j.Fields, j.Tm)
}

func (m *ccMessage) MarshalJSON() ([]byte, error) {
	return m.ToJSON(map[string]bool{})
}

func (m *ccMessage) UnmarshalJSON(data []byte) error {
	var j ccMessageJSON
	err := json.Unmarshal(data, &j)
	if err != nil {
		return fmt.Errorf("failed to parse JSON to CCMessage: %v", err.Error())
	}
	m.name = j.Name
	m.tm = j.Tm
	m.meta = make(map[string]string)
	m.tags = make(map[string]string)
	for k, v := range j.Tags {
		m.tags[k] = v
	}
	m.fields = make(map[string]interface{})
	for k, v := range j.Fields {
		m.fields[k] = v
	}
	return nil
}

func FromBytes(data []byte) ([]CCMessage, error) {
	out := make([]CCMessage, 0)
	decoder := lp2.NewDecoderWithBytes(data)
	for decoder.Next() {
		// Decode measurement name
		measurement, err := decoder.Measurement()
		if err != nil {
			msg := "ccmessage: Failed to decode measurement: " + err.Error()
			return nil, errors.New(msg)
		}

		// Decode tags
		tags := make(map[string]string)
		for {
			key, value, err := decoder.NextTag()
			if err != nil {
				msg := "ccmessage: Failed to decode tag: " + err.Error()
				return nil, errors.New(msg)
			}
			if key == nil {
				break
			}
			tags[string(key)] = string(value)
		}

		// Decode fields
		fields := make(map[string]interface{})
		for {
			key, value, err := decoder.NextField()
			if err != nil {
				msg := "ccmessage: Failed to decode field: " + err.Error()
				return nil, errors.New(msg)
			}
			if key == nil {
				break
			}
			fields[string(key)] = value.Interface()
		}

		// Decode time stamp
		t, err := decoder.Time(lp2.Nanosecond, time.Time{})
		if err != nil {
			msg := "ccmessage: Failed to decode time: " + err.Error()
			return nil, errors.New(msg)
		}

		y, err := NewMessage(
			string(measurement),
			tags,
			map[string]string{},
			fields,
			t,
		)
		if err != nil {
			msg := "ccmessage: Failed to create CCMessage: " + err.Error()
			return nil, errors.New(msg)
		}
		out = append(out, y)
	}
	return out, nil
}

func (m *ccMessage) Bytes() ([]byte, error) {
	var encoder lp2.Encoder
	encoder.SetPrecision(lp2.Nanosecond)

	sortedkeys := make([]string, 0)
	for k := range m.Tags() {
		sortedkeys = append(sortedkeys, k)
	}
	sort.Strings(sortedkeys)

	encoder.StartLine(m.Name())
	for _, k := range sortedkeys {
		v, ok := m.GetTag(k)
		if !ok {
			msg := fmt.Sprintf("CCMessage: Failed to get tag for key %s", k)
			return nil, errors.New(msg)
		}
		encoder.AddTag(k, v)
	}
	for k, v := range m.Fields() {
		nv, ok := lp2.NewValue(v)
		if !ok {
			msg := fmt.Sprintf("CCMessage: Failed to get field value for key %s", k)
			return nil, errors.New(msg)
		}
		encoder.AddField(k, nv)
	}
	encoder.EndLine(m.Time())
	if err := encoder.Err(); err != nil {
		msg := fmt.Sprintf("CCMessage: Failed to encode message: %v", err.Error())
		return nil, errors.New(msg)
	}
	return encoder.Bytes(), nil
}

func (m *ccMessage) MessageType() CCMessageType {
	if m.HasField("value") {
		return CCMSG_TYPE_METRIC
	} else if m.HasField("event") {
		return CCMSG_TYPE_EVENT
	} else if m.HasField("log") {
		return CCMSG_TYPE_LOG
	} else if m.HasField("control") {
		return CCMSG_TYPE_CONTROL
	}
	return CCMSG_TYPE_INVALID
}

// convertField converts data types of fields by the following schemata:
//
//	                       *float32, *float64,                      float32, float64 -> float64
//	*int,  *int8,  *int16,   *int32,   *int64,  int,  int8,  int16,   int32,   int64 ->   int64
//
// *uint, *uint8, *uint16,  *uint32,  *uint64, uint, uint8, uint16,  uint32,  uint64 ->  uint64
// *[]byte, *string,                           []byte, string                        -> string
// *bool,                                      bool                                  -> bool
func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case float64:
		return v
	case int64:
		return v
	case string:
		return v
	case bool:
		return v
	case int:
		return int64(v)
	case uint:
		return uint64(v)
	case uint64:
		return uint64(v)
	case []byte:
		return string(v)
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint32:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint8:
		return uint64(v)
	case float32:
		return float64(v)
	case *float64:
		if v != nil {
			return *v
		}
	case *int64:
		if v != nil {
			return *v
		}
	case *string:
		if v != nil {
			return *v
		}
	case *bool:
		if v != nil {
			return *v
		}
	case *int:
		if v != nil {
			return int64(*v)
		}
	case *uint:
		if v != nil {
			return uint64(*v)
		}
	case *uint64:
		if v != nil {
			return uint64(*v)
		}
	case *[]byte:
		if v != nil {
			return string(*v)
		}
	case *int32:
		if v != nil {
			return int64(*v)
		}
	case *int16:
		if v != nil {
			return int64(*v)
		}
	case *int8:
		if v != nil {
			return int64(*v)
		}
	case *uint32:
		if v != nil {
			return uint64(*v)
		}
	case *uint16:
		if v != nil {
			return uint64(*v)
		}
	case *uint8:
		if v != nil {
			return uint64(*v)
		}
	case *float32:
		if v != nil {
			return float64(*v)
		}
	default:
		return nil
	}
	return nil
}
