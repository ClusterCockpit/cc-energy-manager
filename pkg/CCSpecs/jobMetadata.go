package ccspecs

import (
	"encoding/json"
	"fmt"
	"strings"
)

type CCJobResources struct {
	Hostname      string   `json:"hostname"`
	HWThreads     []int    `json:"hwthreads,omitempty"`
	Accelerators  []string `json:"accelerators,omitempty"`
	Configuration []string `json:"configuration,omitempty"`
}

type CCJobMetaData struct {
	JobScript string `json:"jobScript"`
	JobName   string `json:"jobName"`
	SlurmInfo string `json:"slurmInfo"`
}

type CCJobState string

const (
	CC_JOB_STATE_COMPLETED   CCJobState = "completed"
	CC_JOB_STATE_FAILED      CCJobState = "failed"
	CC_JOB_STATE_CANCELLED   CCJobState = "cancelled"
	CC_JOB_STATE_STOPPED     CCJobState = "stopped"
	CC_JOB_STATE_OUTOFMEMORY CCJobState = "out_of_memory"
	CC_JOB_STATE_TIMEOUT     CCJobState = "timeout"
)

func (s *CCJobState) UnmarshalJSON(b []byte) error {
	var sb string

	err := json.Unmarshal(b, &sb)
	if err != nil {
		return err
	}
	switch sb {
	case string(CC_JOB_STATE_COMPLETED):
		*s = CC_JOB_STATE_COMPLETED
	case string(CC_JOB_STATE_FAILED):
		*s = CC_JOB_STATE_FAILED
	case string(CC_JOB_STATE_CANCELLED):
		*s = CC_JOB_STATE_CANCELLED
	case string(CC_JOB_STATE_STOPPED):
		*s = CC_JOB_STATE_STOPPED
	case string(CC_JOB_STATE_OUTOFMEMORY):
		*s = CC_JOB_STATE_OUTOFMEMORY
	case string(CC_JOB_STATE_TIMEOUT):
		*s = CC_JOB_STATE_TIMEOUT
	default:
		return fmt.Errorf("cannot unmarshal invalid job state '%s'", sb)
	}
	return nil
}

func (s CCJobState) MarshalJSON() ([]byte, error) {
	switch s {
	case CC_JOB_STATE_COMPLETED:
		return json.Marshal(string(CC_JOB_STATE_COMPLETED))
	case CC_JOB_STATE_FAILED:
		return json.Marshal(string(CC_JOB_STATE_FAILED))
	case CC_JOB_STATE_CANCELLED:
		return json.Marshal(string(CC_JOB_STATE_CANCELLED))
	case CC_JOB_STATE_STOPPED:
		return json.Marshal(string(CC_JOB_STATE_STOPPED))
	case CC_JOB_STATE_OUTOFMEMORY:
		return json.Marshal(string(CC_JOB_STATE_OUTOFMEMORY))
	case CC_JOB_STATE_TIMEOUT:
		return json.Marshal(string(CC_JOB_STATE_TIMEOUT))
	}
	return nil, fmt.Errorf("cannot marshall invalid state '%s'", s)
}

func (s CCJobState) String() string {
	return string(s)
}

type CCJob struct {
	JobID            int64            `json:"jobid"`
	User             string           `json:"user"`
	Project          string           `json:"project"`
	Cluster          string           `json:"cluster"`
	SubCluster       string           `json:"subCluster"`
	Partition        string           `json:"partition,omitempty"`
	ArrayJobID       int64            `json:"arrayJobId,omitempty"`
	NumNodes         int64            `json:"numNodes"`
	NumHWThreads     int64            `json:"numHwthreads,omitempty"`
	NumAcc           int64            `json:"numAcc,omitempty"`
	Exclusive        int              `json:"exclusive"`
	MonitoringStatus int              `json:"monitoringStatus,omitempty"`
	Smt              int64            `json:"smt,omitempty"`
	Walltime_in_s    int64            `json:"walltime,omitempty"`
	JobState         CCJobState       `json:"jobState"`
	StartTime        int64            `json:"startTime"`
	Duration         int64            `json:"duration"`
	Resources        []CCJobResources `json:"resources"`
}

func LoadCCJob(input json.RawMessage) (CCJob, error) {
	var j CCJob
	err := json.Unmarshal(input, &j)
	return j, err
}

func MarshalCCJob(job CCJob) (json.RawMessage, error) {
	return json.Marshal(job)
}

func (j *CCJob) String() string {
	lines := make([]string, 0)
	lines = append(lines, fmt.Sprintf("JobID: %d", j.JobID))
	if j.ArrayJobID > 0 {
		lines = append(lines, fmt.Sprintf("ArrayJobID: %d", j.ArrayJobID))
	}
	lines = append(lines, fmt.Sprintf("Cluster: %s (%s)", j.Cluster, j.SubCluster))
	lines = append(lines, fmt.Sprintf("NumNodes: %d", j.NumNodes))
	if j.NumHWThreads > 0 {
		lines = append(lines, fmt.Sprintf("NumHWThreads: %d", j.NumHWThreads))
	}
	if j.NumAcc > 0 {
		lines = append(lines, fmt.Sprintf("NumAcc: %d", j.NumAcc))
	}
	if len(j.Resources) > 0 {
		lines = append(lines, "Resources:")
		for _, r := range j.Resources {
			lines = append(lines, fmt.Sprintf("\tHostname: %s", r.Hostname))
			hwlist := make([]string, 0)
			if len(r.HWThreads) > 0 {
				for _, h := range r.HWThreads {
					hwlist = append(hwlist, fmt.Sprintf("%d", h))
				}
				lines = append(lines, fmt.Sprintf("\t\tHWThreads: [%s]", strings.Join(hwlist, ",")))
			}
			if len(r.Accelerators) > 0 {
				lines = append(lines, fmt.Sprintf("\t\tAccelerators: [%s]", strings.Join(r.Accelerators, ",")))
			}
		}
	}
	return strings.Join(lines, "\n")
}
