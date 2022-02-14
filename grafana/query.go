package grafana

func Query() QueryResponse {
	return []QueryResponseMetric{
		{
			Target: "total_daus",
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
		{
			Target: "new_daus",
			Datapoints: []Datapoint{
				[]float64{
					316,
					1640995200 * 1000,
				},
				[]float64{
					378,
					1641081600 * 1000,
				},
				[]float64{
					370,
					1641254400 * 1000,
				},
			},
		},
	}
}
