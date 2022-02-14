package grafana

import (
	"gametaverse-data-service/schema"
)

func GetDailyTransactionVolumeMetrics(dailyTransactionVolumes []schema.DailyTransactionVolume) QueryResponse {
	renteeDatapoints := make([]Datapoint, len(dailyTransactionVolumes))
	purchaserDatapoints := make([]Datapoint, len(dailyTransactionVolumes))
	withdrawerDatapoints := make([]Datapoint, len(dailyTransactionVolumes))
	for i, dailyTransactionVolume := range dailyTransactionVolumes {
		timestamp := dailyTransactionVolume.DateTimestamp * 1000
		renteeDatapoints[i] = []float64{float64(dailyTransactionVolume.TotalTransactionVolume.RenterTransactionVolume), float64(timestamp)}
		purchaserDatapoints[i] = []float64{float64(dailyTransactionVolume.TotalTransactionVolume.PurchaserTransactionVolume), float64(timestamp)}
		withdrawerDatapoints[i] = []float64{float64(dailyTransactionVolume.TotalTransactionVolume.WithdrawerTransactionVolume), float64(timestamp)}
	}
	return []QueryResponseMetric{
		{
			Target:     "renteeTransactionVolume",
			Datapoints: renteeDatapoints,
		},
		{
			Target:     "purchaserTransactionVolume",
			Datapoints: purchaserDatapoints,
		},
		{
			Target:     "withdrawerTransactionVolume",
			Datapoints: withdrawerDatapoints,
		},
	}
}
