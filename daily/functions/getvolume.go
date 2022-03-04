package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetTransactionVolumes(s3client *s3.S3, start time.Time, end time.Time) []schema.DailyTransactionVolume {
	res := make([]schema.DailyTransactionVolume, 0)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		s := GetTransactionVolume(s3client, d)
		res = append(res, s)
	}

	return res
}

func GetTransactionVolume(s3client *s3.S3, t time.Time) schema.DailyTransactionVolume {
	date := t.Format(schema.DateFormat)
	ac := lib.GetUserActions(s3client, date)
	var r, p, w int64
	for _, as := range ac {
		for _, a := range as {
			if a.Action == schema.UserActionRentSharkSEA {
				r += a.Value.(int64)
			} else if a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				p += a.Value.(int64)
			} else if a.Action == schema.UserActionWithdrawlSEA {
				w += a.Value.(int64)
			}
		}
	}
	return schema.DailyTransactionVolume{
		DateTimestamp: t.Unix(),
		TotalTransactionVolume: schema.UserTransactionVolume{
			RenterTransactionVolume:     r,
			PurchaserTransactionVolume:  p,
			WithdrawerTransactionVolume: w,
		},
	}
}
