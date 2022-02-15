package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserSpendingUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserSpendingUsdDistributionDatapoints := make([]Datapoint, 0)
	for _, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		// To delete
		if userRoiDetail.TotalSpendingUsd > 500 {
			continue
		}
		newUserSpendingUsdDistributionDatapoints = append(newUserSpendingUsdDistributionDatapoints, []float64{float64(userRoiDetail.TotalSpendingUsd), 10})
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserSpendingUsdDistribution",
			Datapoints: newUserSpendingUsdDistributionDatapoints,
		},
	}
}

func GetNewUserProfitUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserProfitUsdDistributionDatapoints := make([]Datapoint, 0)
	for _, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		// To delete
		if userRoiDetail.TotalProfitUsd > 1000 || userRoiDetail.TotalProfitUsd < -1000 {
			continue
		}
		newUserProfitUsdDistributionDatapoints = append(newUserProfitUsdDistributionDatapoints, []float64{float64(userRoiDetail.TotalProfitUsd), 0})
	}
	return []QueryResponseMetric{
		{
			Target:     "newUserProfitUsdDistribution",
			Datapoints: newUserProfitUsdDistributionDatapoints,
		},
	}
}
