package grafana

import (
	"gametaverse-data-service/schema"
)

func GetUserActualActiveDatesDistributionMetrics(userActivities []schema.UserActivity) QueryResponse {
	actualActiveDaysDistributionDatapoints := make([]Datapoint, len(userActivities))
	for i, userActivity := range userActivities {
		actualActiveDaysDistributionDatapoints[i] = []float64{float64(userActivity.ActiveDatesCount), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "actualActiveDaysDistribution",
			Datapoints: actualActiveDaysDistributionDatapoints,
		},
	}
}

func GetUserTotalActiveDatesDistributionMetrics(userActivities []schema.UserActivity) QueryResponse {
	totalActiveDaysDistributionDatapoints := make([]Datapoint, len(userActivities))
	for i, userActivity := range userActivities {
		totalActiveDaysDistributionDatapoints[i] = []float64{float64(userActivity.TotalDatesCount), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "TotalActiveDaysDistribution",
			Datapoints: totalActiveDaysDistributionDatapoints,
		},
	}
}
