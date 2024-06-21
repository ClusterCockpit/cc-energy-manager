package ccspecs

import (
	"encoding/json"
	"os"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	var job CCJob
	testfile := "testjob.json"
	jobFile, err := os.Open(testfile)
	if err != nil {
		t.Errorf("failed to open %s: %v", testfile, err.Error())
		return
	}
	defer jobFile.Close()
	jsonParser := json.NewDecoder(jobFile)
	err = jsonParser.Decode(&job)
	if err != nil {
		t.Errorf("failed to decode %s: %v", testfile, err.Error())
		return
	}
	t.Log(job.String())
}
