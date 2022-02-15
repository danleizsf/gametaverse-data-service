package grafana

import (
	"gametaverse-data-service/schema"
)

func GetNewUserTypeMetrics(newUserTypes schema.UserTypeCount) QueryResponse {
	return []QueryResponseMetric{
		{
			Target:     "renteeCount",
			Datapoints: []Datapoint{[]float64{float64(newUserTypes.RenteeCount), float64(0)}},
		},
		{
			Target:     "purchaserCount",
			Datapoints: []Datapoint{[]float64{float64(newUserTypes.PurchaserCount), float64(0)}},
		},
		{
			Target:     "HybriderCount",
			Datapoints: []Datapoint{[]float64{float64(newUserTypes.HybridCount), float64(0)}},
		},
	}
}
