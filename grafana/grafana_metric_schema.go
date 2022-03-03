package grafana

type QueryResponse []QueryResponseMetric

type QueryResponseMetric struct {
	Target     string      `json:"target"`
	Datapoints []Datapoint `json:"datapoints"`
}

type TableMetrics struct {
	Type    string   `json:"type"`
	Columns []Column `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type Column struct {
	Text string `json:"text"`
	Type string `json:"type"`
}

type Row []interface{}

type Datapoint []float64
