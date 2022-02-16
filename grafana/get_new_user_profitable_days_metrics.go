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

func GetNewRenteeProfitableDaysDistributionMetrics(userRois []schema.UserRoiDetail) QueryResponse {
	renteeCount := 0
	for _, userRoi := range userRois {
		if userRoi.UserType == schema.Rentee {
			renteeCount += 1
		}
	}
	newRenteeProfitableDaysDistributionDatapoints := make([]Datapoint, renteeCount)
	idx := 0
	for _, userRoi := range userRois {
		if userRoi.UserType == schema.Rentee {
			newRenteeProfitableDaysDistributionDatapoints[idx] = []float64{float64(userRoi.ProfitableDays), 0}
			idx += 1
		}
	}
	return []QueryResponseMetric{
		{
			Target:     "newRenteeProfitableDaysDistribution",
			Datapoints: newRenteeProfitableDaysDistributionDatapoints,
		},
	}
}

func GetNewPurchaserProfitableDaysDistributionMetrics(userRois []schema.UserRoiDetail) QueryResponse {
	purchaserCount := 0
	for _, userRoi := range userRois {
		if userRoi.UserType == schema.Purchaser {
			purchaserCount += 1
		}
	}
	newPurchaserProfitableDaysDistributionDatapoints := make([]Datapoint, purchaserCount)
	idx := 0
	for _, userRoi := range userRois {
		if userRoi.UserType == schema.Purchaser {
			newPurchaserProfitableDaysDistributionDatapoints[idx] = []float64{float64(userRoi.ProfitableDays), 0}
			idx += 1
		}
	}
	return []QueryResponseMetric{
		{
			Target:     "newPurchaserProfitableDaysDistribution",
			Datapoints: newPurchaserProfitableDaysDistributionDatapoints,
		},
	}
}
