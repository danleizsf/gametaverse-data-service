package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail, sortType schema.WhalesSortType) []TableMetrics {
	whaleRoisDatapoints := make([]Row, 0)
	for _, whaleRoi := range whaleRois {
		if sortType == schema.SortByGain {
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalGainUsd,
				whaleRoi.TotalGainToken,
			})
		} else if sortType == schema.SortByProfit {
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalProfitUsd,
				whaleRoi.TotalProfitToken,
			})
		} else if sortType == schema.SortBySpending {
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalSpendingUsd,
				whaleRoi.TotalSpendingToken,
			})
		}
	}
	return []TableMetrics{
		{
			Type: "table",
			Columns: []Column{
				{
					Text: "Whale address",
					Type: "string",
				},
				{
					Text: "USD",
					Type: "number",
				},
				{
					Text: "SEA",
					Type: "number",
				},
			},
			Rows: whaleRoisDatapoints,
		},
	}
}
