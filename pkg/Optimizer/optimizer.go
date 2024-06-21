package optimizer

import (
	"encoding/json"
	"sync"
	"time"

	ccspecs "github.com/ClusterCockpit/cc-backend/pkg/schema"
	lp "github.com/ClusterCockpit/cc-metric-collector/pkg/ccMetric"
)

type optimizerConfig struct {
	Type    string   `json:"type"`
	Metrics []string `json:"metrics"`
}

type optimizer struct {
	wg       *sync.WaitGroup
	done     chan bool
	ident    string
	input    chan lp.CCMetric
	output   chan lp.CCMetric
	metadata ccspecs.BaseJob
	ticker   time.Ticker
	started  bool
}

type Optimizer interface {
	Init(ident string, wg *sync.WaitGroup, metadata ccspecs.BaseJob, config json.RawMessage) error
	AddInput(input chan lp.CCMetric)
	AddOutput(output chan lp.CCMetric)
	NewRegion(regionname string)
	CloseRegion(regionname string)
	Start()
	Close()
}

func (os *optimizer) AddInput(input chan lp.CCMetric) {
	os.input = input
}

func (os *optimizer) AddOutput(output chan lp.CCMetric) {
	os.output = output
}

// func (o *optimizer) Init(config json.RawMessage) error {
// 	return nil
// }
// func (o *optimizer) Run(metadata *ccspecs.BaseJob, data []lp.CCMetric) ([]lp.CCControl, error) {
// 	controls := make([]lp.CCControl, 0)
// 	return controls, nil
// }

// func (o *optimizer) Close() {

// }
