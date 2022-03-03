package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail) QueryResponse {
	whaleRoisDatapoints := make([]Datapoint, 0)
	for _, whaleRoi := range whaleRois {
		whaleRoisDatapoints = append(whaleRoisDatapoints, []float64{float64(whaleRoi.TotalGainUsd), 10})
	}
	return []QueryResponseMetric{
		{
			Target:     "whaleRois",
			Datapoints: whaleRoisDatapoints,
		},
	}
}
