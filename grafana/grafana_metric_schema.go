package grafana

type QueryResponse []QueryResponseMetric

type QueryResponseMetric struct {
	Target     string      `json:"target"`
	Datapoints []Datapoint `json:"datapoints"`
}

type Datapoint []float64
