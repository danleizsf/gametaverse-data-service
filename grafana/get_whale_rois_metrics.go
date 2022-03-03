package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail, sortType schema.WhalesSortType) []TableMetrics {
	whaleRoisDatapoints := make([]Row, 0)
	var columns []Column
	for _, whaleRoi := range whaleRois {
		if sortType == schema.SortByGain {
			columns = []Column{
				{
					Text: "Whale address",
					Type: "string",
				},
				{
					Text: "USD gained",
					Type: "number",
				},
				{
					Text: "SEA gained",
					Type: "number",
				},
			}
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalGainUsd,
				whaleRoi.TotalGainToken,
			})
		} else if sortType == schema.SortByProfit {
			columns = []Column{
				{
					Text: "Whale address",
					Type: "string",
				},
				{
					Text: "USD earned",
					Type: "number",
				},
				{
					Text: "SEA earned",
					Type: "number",
				},
			}
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalProfitUsd,
				whaleRoi.TotalProfitToken,
			})
		} else if sortType == schema.SortBySpending {
			columns = []Column{
				{
					Text: "Whale address",
					Type: "string",
				},
				{
					Text: "USD spent",
					Type: "number",
				},
				{
					Text: "SEA spent",
					Type: "number",
				},
			}
			whaleRoisDatapoints = append(whaleRoisDatapoints, Row{
				whaleRoi.UserAddress,
				whaleRoi.TotalSpendingUsd,
				whaleRoi.TotalSpendingToken,
			})
		}
	}
	return []TableMetrics{
		{
			Type:    "table",
			Columns: columns,
			Rows:    whaleRoisDatapoints,
		},
	}
}
