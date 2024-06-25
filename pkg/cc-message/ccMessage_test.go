package ccmessage

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestJSONEncode(t *testing.T) {
	input := []CCMessage{
		&ccMessage{name: "test1", tags: map[string]string{"type": "node"}, meta: map[string]string{"unit": "B"}, fields: map[string]interface{}{"value": 1.23}, tm: time.Now()},
		&ccMessage{name: "test2", tags: map[string]string{"type": "socket", "type-id": "0"}, meta: map[string]string{"unit": "B"}, fields: map[string]interface{}{"value": 1.23}, tm: time.Now()},
	}

	x, err := json.Marshal(input)
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log(string(x))

}

func TestJSONDecode(t *testing.T) {
	input := `[{"name":"test1","tags":{"type":"node"},"fields":{"value":1.23},"timestamp":"2024-06-22T13:51:59.495479906+02:00"},{"name":"test2","tags":{"type":"socket","type-id":"0"},"fields":{"value":1.23},"timestamp":"2024-06-22T13:51:59.495481095+02:00"}]`
	var list []*ccMessage
	///var list []CCMessage
	err := json.Unmarshal([]byte(input), &list)
	if err != nil {
		t.Error(err.Error())
		return
	}
	//t.Log(list)
	for _, m := range list {
		t.Log(m.Name())
	}
}

func TestILPDecode(t *testing.T) {
	input := fmt.Sprintf(`test1,type=node value=1.23 %d
test2,type=socket,type-id=0 value=1.23 %d`, time.Now().UnixNano(), time.Now().UnixNano())

	list, err := FromBytes([]byte(input))
	if err != nil {
		t.Error(err.Error())
		return
	}
	for _, m := range list {
		t.Log(m.Name())
	}
}
