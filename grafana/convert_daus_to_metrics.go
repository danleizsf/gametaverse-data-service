package grafana

import (
	"gametaverse-data-service/schema"
)

func ConverDausToMetrics(daus []schema.Dau) QueryResponse {

	return []QueryResponseMetric{
		ConvertTotalActiveUserCountMetrics(daus),
	}
}

func ConvertTotalActiveUserCountMetrics(daus []schema.Dau) QueryResponseMetric {
	datapoints := make([]Datapoint, len(daus))
	for i, dau := range daus {
		count := dau.TotalActiveUsers.TotalUserCount
		timestamp := dau.DateTimestamp * 1000
		datapoints[i] = []float64{float64(count), float64(timestamp)}
	}
	return QueryResponseMetric{
		Target:     "totalActiveUserCount",
		Datapoints: datapoints,
	}
}

func ConvertNewActiveUserCountMetrics(daus []schema.Dau) QueryResponseMetric {
	datapoints := make([]Datapoint, len(daus))
	for i, dau := range daus {
		count := dau.NewActiveUsers.TotalUserCount
		timestamp := dau.DateTimestamp * 1000
		datapoints[i] = []float64{float64(count), float64(timestamp)}
	}
	return QueryResponseMetric{
		Target:     "newActiveUserCount",
		Datapoints: datapoints,
	}
}
