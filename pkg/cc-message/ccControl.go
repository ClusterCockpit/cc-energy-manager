package ccmessage

import (
	"reflect"
	"time"
)

type CCControl interface {
	CCMessage
}

func NewGetControl(name string,
	tags map[string]string,
	meta map[string]string,
	tm time.Time,
) (CCControl, error) {
	m, err := NewMessage(name, tags, meta, map[string]interface{}{"control": ""}, tm)
	if err == nil {
		m.AddTag("method", "GET")
	}
	return m, err
}

func NewPutControl(name string,
	tags map[string]string,
	meta map[string]string,
	value string,
	tm time.Time,
) (CCControl, error) {
	m, err := NewMessage(name, tags, meta, map[string]interface{}{"control": value}, tm)
	if err == nil {
		m.AddTag("method", "PUT")
	}
	return m, err
}

func IsControl(m CCControl) bool {
	if v, ok := m.GetField("control"); ok {
		if me, ok := m.GetTag("method"); ok {
			if reflect.TypeOf(v) == reflect.TypeOf("string") && (me == "PUT" || me == "GET") {
				return true
			}
		}
	}
	return false
}

func IsControlMessage(m CCMessage) bool {
	return IsControl(m)
}

func GetControlValue(m CCControl) string {
	if IsControl(m) {
		if v, ok := m.GetField("control"); ok {
			return v.(string)
		}
	}
	return ""
}

func GetControlMethod(m CCControl) string {
	if IsControl(m) {
		if v, ok := m.GetTag("method"); ok {
			return v
		}
	}
	return ""
}
