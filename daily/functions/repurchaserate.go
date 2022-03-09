package daily

import (
	"encoding/json"
	"gametaverse-data-service/lib"

	"github.com/aws/aws-sdk-go/service/s3"
)

type Wrapper struct {
	Rate float64
}

func GetUserRepurchaseRate(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64) float64 {
	var resp Wrapper
	key := lib.GetDateRange(timestampA, timestampB)
	if body, exist := lib.GetRangeCacheFromS3(s3client, key, "GetUserRepurchaseRate"); exist {
		json.Unmarshal(body, &resp)
		return resp.Rate
	}
	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	var repurchaseUserCount int
	for _, actions := range useractions {
		if actions[len(actions)-1].Date != actions[0].Date {
			repurchaseUserCount++
		}
	}
	res := float64(repurchaseUserCount) / float64(len(useractions))
	body, _ := json.Marshal(Wrapper{
		Rate: res,
	})
	lib.SetRangeCacheFromS3(s3client, key, "GetUserRepurchaseRate", body)
	return res
}
