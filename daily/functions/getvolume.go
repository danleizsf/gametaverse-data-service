package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetTransactionVolumes(s3client *s3.S3, start time.Time, end time.Time) []schema.DailyTransactionVolume {
	len := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		len++
	}
	res := make([]schema.DailyTransactionVolume, len)
	var wg sync.WaitGroup
	wg.Add(len)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, s3client *s3.S3, d time.Time) {
			defer wg.Done()
			s := GetTransactionVolume(s3client, d)
			res[i] = s
		}(i, s3client, d)
		i++
	}
	wg.Wait()
	return res
}

func GetTransactionVolume(s3client *s3.S3, t time.Time) schema.DailyTransactionVolume {
	date := t.Format(schema.DateFormat)
	ac := lib.GetUserActions(s3client, date)
	var r, p, w float64
	for _, as := range ac {
		for _, a := range as {
			if a.Action == schema.UserActionRentSharkSEA {
				r += a.Value.(float64)
			} else if a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				p += a.Value.(float64)
			} else if a.Action == schema.UserActionWithdrawlSEA {
				w += a.Value.(float64)
			}
		}
	}
	return schema.DailyTransactionVolume{
		DateTimestamp: t.Unix(),
		TotalTransactionVolume: schema.UserTransactionVolume{
			RenterTransactionVolume:     int64(r),
			PurchaserTransactionVolume:  int64(p),
			WithdrawerTransactionVolume: int64(w),
		},
	}
}
