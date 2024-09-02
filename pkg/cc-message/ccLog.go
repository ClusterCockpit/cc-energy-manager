package ccmessage

import (
	"reflect"
	"time"
)

type CCLog interface {
	CCMessage
}

func NewLog(name string,
	tags map[string]string,
	meta map[string]string,
	log string,
	tm time.Time,
) (CCLog, error) {
	return NewMessage(name, tags, meta, map[string]interface{}{"log": log}, tm)
}

func IsLog(m CCLog) bool {
	if v, ok := m.GetField("log"); ok {
		if reflect.TypeOf(v) == reflect.TypeOf("string") {
			return true
		}
	}
	return false
}

func IsLogMessage(m CCMessage) bool {
	return IsLog(m)
}

func GetLogValue(m CCMetric) string {
	if IsLog(m) {
		if v, ok := m.GetField("log"); ok {
			return v.(string)
		}
	}
	return ""
}
