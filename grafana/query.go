package grafana

func Query() DauMetric {
	return DauMetric{
		Target: "daus",
		Daus: []Dau{
			{
				DateTimestamp:         1640995200,
				TotalActiveUsersCount: 1069,
			},
			{
				DateTimestamp:         1641081600,
				TotalActiveUsersCount: 1104,
			},
			{
				DateTimestamp:         1641254400,
				TotalActiveUsersCount: 1114,
			},
		},
	}
}
