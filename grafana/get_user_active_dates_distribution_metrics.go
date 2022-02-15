package grafana

import (
	"gametaverse-data-service/schema"
)

func GetUserActiveDatesDistributionMetrics(userActivities []schema.UserActivity) QueryResponse {
	totalActiveDaysDistributionDatapoints := make([]Datapoint, len(userActivities))
	actualActiveDaysDistributionDatapoints := make([]Datapoint, len(userActivities))
	for i, userActivity := range userActivities {
		totalActiveDaysDistributionDatapoints[i] = []float64{float64(userActivity.TotalDatesCount), 0}
		actualActiveDaysDistributionDatapoints[i] = []float64{float64(userActivity.ActiveDatesCount), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "actualActiveDaysDistribution",
			Datapoints: actualActiveDaysDistributionDatapoints,
		},
	}
}
