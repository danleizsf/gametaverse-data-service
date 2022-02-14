package grafana

func Query() QueryResponse {
	return []QueryResponseMetric{
		{
			Target: "daus",
			Datapoints: []Datapoint{
				[]float64{
					1069,
					1640995200 * 1000,
				},
				[]float64{
					1104,
					1641081600 * 1000,
				},
				[]float64{
					1114,
					1641254400 * 1000,
				},
			},
		},
	}
}
