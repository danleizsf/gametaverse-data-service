package grafana

import (
	"gametaverse-data-service/schema"
)

func GetDauMetrics(daus []schema.Dau) QueryResponse {
	newRenteeDatapoints := make([]Datapoint, len(daus))
	totalRenteeDatapoints := make([]Datapoint, len(daus))
	newPurchaserDatapoints := make([]Datapoint, len(daus))
	totalPurchaserDatapoints := make([]Datapoint, len(daus))
	newUserDatapoints := make([]Datapoint, len(daus))
	totalUserDatapoints := make([]Datapoint, len(daus))
	for i, dau := range daus {
		timestamp := dau.DateTimestamp * 1000
		newRenteeDatapoints[i] = []float64{float64(dau.NewActiveUsers.PayerCount.RenteeCount), float64(timestamp)}
		totalRenteeDatapoints[i] = []float64{float64(dau.TotalActiveUsers.PayerCount.RenteeCount), float64(timestamp)}
		newPurchaserDatapoints[i] = []float64{float64(dau.NewActiveUsers.PayerCount.PurchaserCount), float64(timestamp)}
		totalPurchaserDatapoints[i] = []float64{float64(dau.TotalActiveUsers.PayerCount.PurchaserCount), float64(timestamp)}
		newUserDatapoints[i] = []float64{float64(dau.NewActiveUsers.TotalUserCount), float64(timestamp)}
		totalUserDatapoints[i] = []float64{float64(dau.TotalActiveUsers.TotalUserCount), float64(timestamp)}
	}
	return []QueryResponseMetric{
		{
			Target:     "newActiveRenteeCount",
			Datapoints: newRenteeDatapoints,
		},
		{
			Target:     "totalActiveRenteeCount",
			Datapoints: totalRenteeDatapoints,
		},
		{
			Target:     "newActivePurchaserCount",
			Datapoints: newPurchaserDatapoints,
		},
		{
			Target:     "totalActivePurchaserCount",
			Datapoints: totalPurchaserDatapoints,
		},
		{
			Target:     "newActiveUserCount",
			Datapoints: newUserDatapoints,
		},
		{
			Target:     "totalActiveUserCount",
			Datapoints: totalUserDatapoints,
		},
	}
}
