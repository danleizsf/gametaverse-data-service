package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserSpendingUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserSpendingUsdDistributionDatapoints := make([]Datapoint, len(allUserRoiDetails.UserRoiDetails))
	for i, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		newUserSpendingUsdDistributionDatapoints[i] = []float64{float64(userRoiDetail.TotalSpendingUsd), 10}
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserSpendingUsdDistribution",
			Datapoints: newUserSpendingUsdDistributionDatapoints,
		},
	}
}

func GetNewUserProfitUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserProfitUsdDistributionDatapoints := make([]Datapoint, len(allUserRoiDetails.UserRoiDetails))
	for i, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		newUserProfitUsdDistributionDatapoints[i] = []float64{float64(userRoiDetail.TotalProfitUsd), 0}
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserProfitUsdDistribution",
			Datapoints: newUserProfitUsdDistributionDatapoints,
		},
	}
}
