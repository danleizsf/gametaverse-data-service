package grafana

func GetNewUserProfitableRateMetrics(newUserProfitableRate float64) QueryResponse {
	return []QueryResponseMetric{
		{
			Target:     "newUserProfitableRate",
			Datapoints: []Datapoint{[]float64{newUserProfitableRate, float64(0)}},
		},
	}
}
