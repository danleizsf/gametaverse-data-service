package grafana

func GetUserRepurchaseRateMetrics(userRepurchaseRate float64) QueryResponse {
	return []QueryResponseMetric{
		{
			Target:     "userRepurchaseRate",
			Datapoints: []Datapoint{[]float64{userRepurchaseRate, float64(0)}},
		},
	}
}
