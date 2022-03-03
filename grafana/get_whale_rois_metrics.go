package grafana

import (
	"gametaverse-data-service/schema"
)

func GetWhaleRoisMetrics(whaleRois []schema.UserRoiDetail) []TableMetrics {
	whaleRoisDatapoints := make([]Row, 0)
	for _, whaleRoi := range whaleRois {
		whaleRoisDatapoints = append(whaleRoisDatapoints, Row{whaleRoi.UserAddress, whaleRoi.TotalGainUsd})
	}
	return []TableMetrics{
		{
			Type: "table",
			Columns: []Column{
				{
					Text: "User address",
					Type: "string",
				},
				{
					Text: "Total gain",
					Type: "number",
				},
			},
			Rows: whaleRoisDatapoints,
		},
	}
}
