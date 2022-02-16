package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserSpendingUsdDistributionMetrics(allUserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newUserSpendingUsdDistributionDatapoints := make([]Datapoint, 0)
	for _, userRoiDetail := range allUserRoiDetails.UserRoiDetails {
		// To delete
		if userRoiDetail.TotalSpendingUsd > 500 || userRoiDetail.TotalSpendingUsd == 0 {
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

func GetNewRenteeProfitUsdDistributionMetrics(allRenteeRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newRenteeProfitUsdDistributionDatapoints := make([]Datapoint, 0)
	for _, renteeRoiDetail := range allRenteeRoiDetails.UserRoiDetails {
		// To delete
		if renteeRoiDetail.TotalProfitUsd > 1000 || renteeRoiDetail.TotalProfitUsd < -1000 {
			continue
		}
		if renteeRoiDetail.UserType != schema.Rentee {
			continue
		}
		newRenteeProfitUsdDistributionDatapoints = append(newRenteeProfitUsdDistributionDatapoints, []float64{float64(renteeRoiDetail.TotalProfitUsd), 0})
	}
	return []QueryResponseMetric{
		{
			Target:     "newRenteeProfitUsdDistribution",
			Datapoints: newRenteeProfitUsdDistributionDatapoints,
		},
	}
}

func GetNewPurchaserProfitUsdDistributionMetrics(allPurchaserRoiDetails schema.AllUserRoiDetails) QueryResponse {
	newPurchaserProfitUsdDistributionDatapoints := make([]Datapoint, 0)
	for _, purchaserRoiDetail := range allPurchaserRoiDetails.UserRoiDetails {
		// To delete
		if purchaserRoiDetail.TotalProfitUsd > 1000 || purchaserRoiDetail.TotalProfitUsd < -1000 {
			continue
		}
		if purchaserRoiDetail.UserType != schema.Purchaser {
			continue
		}
		newPurchaserProfitUsdDistributionDatapoints = append(newPurchaserProfitUsdDistributionDatapoints, []float64{float64(purchaserRoiDetail.TotalProfitUsd), 0})
	}
	return []QueryResponseMetric{
		{
			Target:     "newPurchaserProfitUsdDistribution",
			Datapoints: newPurchaserProfitUsdDistributionDatapoints,
		},
	}
}
