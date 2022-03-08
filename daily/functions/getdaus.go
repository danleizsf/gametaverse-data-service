package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetDaus(s3client *s3.S3, cache *lib.Cache, start time.Time, end time.Time) []schema.Dau {
	len := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		len++
	}
	summarys := lib.GetSummaryRangeAsync(s3client, cache, start.Unix(), end.Unix())
	useractions := lib.GetUserActionsRangeAsyncByDate(s3client, cache, start.Unix(), end.Unix())
	res := make([]schema.Dau, len+1)
	var wg sync.WaitGroup
	wg.Add(len)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, d time.Time) {
			defer wg.Done()
			s := GetDau(summarys[i], useractions[i], d)
			res[i] = s
		}(i, d)
		i++
	}
	wg.Wait()
	return res
}

func sliceToMap(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, ss := range s {
		m[ss] = true
	}
	return m
}
func GetDau(s schema.Summary, ac map[string][]schema.UserAction, d time.Time) schema.Dau {
	newUser := sliceToMap(s.NewUser)

	var np, nr, tp, tr int64
	for u, a := range ac {
		payerType := GetPerPayerType(a)
		if payerType == schema.Purchaser {
			if _, exists := newUser[u]; exists {
				np++
			} else {
				tp++
			}
		}
		if payerType == schema.Rentee {
			if _, exists := newUser[u]; exists {
				nr++
			} else {
				tr++
			}
		}

	}
	return schema.Dau{
		DateTimestamp: d.Unix(),
		NewActiveUsers: schema.ActiveUserCount{
			PayerCount: schema.PayerCount{
				RenteeCount:    nr,
				PurchaserCount: np,
			},
			TotalUserCount: int64(len(s.NewUser)),
		},
		TotalActiveUsers: schema.ActiveUserCount{
			PayerCount: schema.PayerCount{
				RenteeCount:    tr,
				PurchaserCount: tp,
			},
			TotalUserCount: int64(len(s.ActiveUser)),
		},
	}
}

func GetPerPayerType(ua []schema.UserAction) schema.PayerType {
	totalRentingValue := float64(0)
	totalInvestingValue := float64(0)
	for _, a := range ua {
		if a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
			totalInvestingValue += a.Value.(float64)
		} else if a.Action == schema.UserActionRentSharkSEA {
			totalRentingValue += a.Value.(float64)
		}
	}
	if totalInvestingValue > totalRentingValue {
		return schema.Purchaser
	} else {
		return schema.Rentee
	}
}
