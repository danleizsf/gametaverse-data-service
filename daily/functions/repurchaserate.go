package daily

import (
	"gametaverse-data-service/lib"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserRepurchaseRate(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64) float64 {
	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	var repurchaseUserCount int
	for _, actions := range useractions {
		if actions[len(actions)-1].Date != actions[0].Date {
			repurchaseUserCount++
		}
	}

	return float64(repurchaseUserCount) / float64(len(useractions))
}
