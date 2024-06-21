package ccmessage

import (
	"reflect"
	"time"
)

type CCEvent interface {
	CCMessage
}

func NewEvent(name string,
	tags map[string]string,
	meta map[string]string,
	value string,
	tm time.Time,
) (CCEvent, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"value": value}, tm)
}

func IsEvent(m CCEvent) bool {
	if v, ok := m.GetField("value"); ok {
		if _, ok := m.GetTag("method"); !ok {
			if reflect.TypeOf(v) == reflect.TypeOf("string") {
				return true
			}
		}
	}
	return false
}

func IsEventMessage(m CCMessage) bool {
	return IsEvent(m)
}

func GetEventValue(m CCMetric) string {
	if IsEvent(m) {
		if v, ok := m.GetField("value"); ok {
			return v.(string)
		}
	}
	return ""
}
