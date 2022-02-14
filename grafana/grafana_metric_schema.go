package grafana

type DauMetric struct {
	Target string `json:"target"`
	Daus   []Dau  `json:"daus"`
}

type Dau struct {
	DateTimestamp         int64 `json:"dateTimestamp"`
	TotalActiveUsersCount int64 `json:"totalActiveUsersCount"`
}
