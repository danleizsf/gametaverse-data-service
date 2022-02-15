package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserProfitableDaysDistributionMetrics(userRois []schema.UserRoiDetail) QueryResponse {
	newUserProfitableDaysDistributionDatapoints := make([]Datapoint, len(userRois))
	for i, userRoi := range userRois {
		newUserProfitableDaysDistributionDatapoints[i] = []float64{float64(userRoi.ProfitableDays), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserProfitableDaysDistribution",
			Datapoints: newUserProfitableDaysDistributionDatapoints,
		},
	}
}
