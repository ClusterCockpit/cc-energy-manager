package ccmessage

import (
	"reflect"
	"time"
)

type CCMetric interface {
	CCMessage
}

func NewMetric(name string,
	tags map[string]string,
	meta map[string]string,
	value interface{},
	tm time.Time,
) (CCMetric, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"value": value}, tm)
}

func IsMetric(m CCMetric) bool {
	if v, ok := m.GetField("value"); ok {
		if reflect.TypeOf(v) != reflect.TypeOf("string") {
			return true
		}
	}
	return false
}
func IsMetricMessage(m CCMessage) bool {
	return IsMetric(m)
}

func GetMetricValue(m CCMetric) interface{} {
	if IsMetric(m) {
		if v, ok := m.GetField("value"); ok {
			return v
		}
	}
	return nil
}
