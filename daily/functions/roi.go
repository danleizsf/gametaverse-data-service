package daily

import (
	"encoding/json"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserRoi(s3client *s3.S3, cache *lib.Cache, start time.Time, end time.Time) []schema.UserRoiDetail {
	var resp []schema.UserRoiDetail
	key := lib.GetDateRange(start.Unix(), end.Unix())
	if body, exist := lib.GetRangeCacheFromS3(s3client, key, "GetNewUserRoi"); exist {
		json.Unmarshal(body, &resp)
		return resp
	}

	summarys := lib.GetSummaryRangeAsync(s3client, cache, start.Unix(), end.Unix())
	newUsers := map[string]bool{}
	for _, dailySummary := range summarys {
		for _, newUser := range dailySummary.NewUser {
			newUsers[newUser] = true
		}
	}
	userActions := lib.GetUserActionsRangeAsync(s3client, cache, start.Unix(), end.Unix())

	userRois := make([]schema.UserRoiDetail, 0)
	for user, actions := range userActions {
		if !newUsers[user] {
			continue
		}
		payerType := UserType(actions)
		var value float64
		profitableIndex := -1

		for i, a := range actions {
			if a.Action == schema.UserActionRentSharkSEA || a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				value -= a.Value.(float64)
			} else if a.Action == schema.UserActionLendSharkSEA || a.Action == schema.UserActionAuctionSellSEA || a.Action == schema.UserActionWithdrawlSEA {
				value += a.Value.(float64)
			}
			if value > 0 {
				profitableIndex = i
				break
			}
		}
		if value <= 0 {
			continue
		}

		firstDate, _ := time.Parse(schema.DateFormat, actions[0].Date)
		profitableDate, _ := time.Parse(schema.DateFormat, actions[profitableIndex].Date)
		profitableDays := int64(math.Ceil(profitableDate.Sub(firstDate).Hours() / 24))
		userRois = append(userRois, schema.UserRoiDetail{
			ProfitableDays: profitableDays,
			UserType:       payerType,
		})
	}
	body, _ := json.Marshal(userRois)
	lib.SetRangeCacheFromS3(s3client, key, "GetNewUserRoi", body)
	return userRois

}
