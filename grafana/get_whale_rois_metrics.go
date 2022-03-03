package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail) []TableMetrics {
	whaleRoisDatapoints := make([]Row, 0)
	for _, whaleRoi := range whaleRois {
		whaleRoisDatapoints = append(whaleRoisDatapoints, Row{whaleRoi.UserAddress, whaleRoi.TotalGainUsd, whaleRoi.TotalProfitUsd, whaleRoi.TotalSpendingUsd})
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
					Text: "Total gain (USD)",
					Type: "number",
				},
				{
					Text: "Total profit (USD)",
					Type: "number",
				},
				{
					Text: "Total spending (USD)",
					Type: "number",
				},
			},
			Rows: whaleRoisDatapoints,
		},
	}
}
