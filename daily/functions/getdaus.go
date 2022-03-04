package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetDaus(s3client *s3.S3, start time.Time, end time.Time) []schema.Dau {
	res := make([]schema.Dau, 0)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		s := GetDau(s3client, d)
		res = append(res, s)
	}

	return res
}

func GetDau(s3client *s3.S3, t time.Time) schema.Dau {
	date := t.Format(schema.DateFormat)
	s := lib.GetSummary(s3client, date)
	return schema.Dau{
		DateTimestamp: t.Unix(),
		NewActiveUsers: schema.ActiveUserCount{
			TotalUserCount: int64(len(s.NewUser)),
		},
		TotalActiveUsers: schema.ActiveUserCount{
			TotalUserCount: int64(len(s.ActiveUser)),
		},
	}
}
