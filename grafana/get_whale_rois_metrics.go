package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail) []TableMetrics {
	whaleRoisDatapoints := make([]Row, 0)
	for _, whaleRoi := range whaleRois {
		whaleRoisDatapoints = append(whaleRoisDatapoints, Row{whaleRoi.UserAddress, whaleRoi.TotalGainUsd, whaleRoi.TotalProfitUsd})
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
					Text: "Total gain",
					Type: "number",
				},
				{
					Text: "Total profit",
					Type: "number",
				},
			},
			Rows: whaleRoisDatapoints,
		},
	}
}
