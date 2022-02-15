package grafana

import (
	"gametaverse-data-service/schema"
)

func GetUserActiveDatesDistributionMetrics(userActivities []schema.UserActivity) QueryResponse {
	totalActiveDaysCounts := map[int64]bool{}
	actualActiveDaysCounts := map[int64]bool{}
	for _, userActivity := range userActivities {
		totalActiveDaysCounts[userActivity.TotalDatesCount] = true
		actualActiveDaysCounts[userActivity.ActiveDatesCount] = true
	}

	totalActiveDaysDistributionDatapoints := make([]Datapoint, len(totalActiveDaysCounts))
	actualActiveDaysDistributionDatapoints := make([]Datapoint, len(actualActiveDaysCounts))
	idx := 0
	for days, _ := range totalActiveDaysCounts {
		totalActiveDaysDistributionDatapoints[idx] = []float64{float64(days), 0}
		idx += 1
	}
	idx = 0
	for days, _ := range actualActiveDaysCounts {
		actualActiveDaysDistributionDatapoints[idx] = []float64{float64(days), 0}
		idx += 1
	}
	return []QueryResponseMetric{
		{
			Target:     "totalActiveDaysDistribution",
			Datapoints: totalActiveDaysDistributionDatapoints,
		},
		{
			Target:     "actualActiveDaysDistribution",
			Datapoints: actualActiveDaysDistributionDatapoints,
		},
	}
}
