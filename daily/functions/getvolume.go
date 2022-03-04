package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"log"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetTransactionVolume(s3client *s3.S3, timestamp int64) schema.DailyTransactionVolume {
	date := lib.GetDate(timestamp)
	log.Print(date)
	s := lib.GetSummary(s3client, date)
	return schema.DailyTransactionVolume{
		DateTimestamp: timestamp,
		TotalTransactionVolume: schema.UserTransactionVolume{
			RenterTransactionVolume:    s.RentSharkVolume,
			PurchaserTransactionVolume: s.CreateSharkVolume + s.BuySharkVolume + s.AuctionSharkVolume,
			TokenVolume:                s.SeaVolume,
		},
	}
}
