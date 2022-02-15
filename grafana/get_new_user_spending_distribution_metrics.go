package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserSpendingUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserSpendingUsdDistributionDatapoints := make([]Datapoint, len(allUserRoiDetails.UserRoiDetails))
	for i, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		newUserSpendingUsdDistributionDatapoints[i] = []float64{float64(userRoiDetail.TotalSpendingUsd), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserSpendingUsdDistribution",
			Datapoints: newUserSpendingUsdDistributionDatapoints,
		},
	}
}
