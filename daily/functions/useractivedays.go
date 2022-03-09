package daily

import (
	"encoding/json"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserActiveDays(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64, limit int64) []schema.UserActivity {
	var resp []schema.UserActivity

	key := lib.GetDateRange(timestampA, timestampB)
	if body, exist := lib.GetRangeCacheFromS3(s3client, key, "GetUserActiveDays"); exist {
		json.Unmarshal(body, &resp)
		return resp
	}

	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	perUserActivities := make(map[string]schema.UserActivity, len(useractions))

	for userAddress, actions := range useractions {
		firstDate, _ := time.Parse(schema.DateFormat, actions[0].Date)
		lastDate, _ := time.Parse(schema.DateFormat, actions[len(actions)-1].Date)
		activeDays := map[string]bool{}
		for _, a := range actions {
			activeDays[a.Date] = true
		}
		perUserActivities[userAddress] = schema.UserActivity{
			UserAddress:      userAddress,
			TotalDatesCount:  int64(lastDate.Sub(firstDate).Hours()) / 24,
			ActiveDatesCount: int64(len(activeDays)),
		}

	}
	resp = make([]schema.UserActivity, len(perUserActivities))
	i := 0
	for _, ac := range perUserActivities {
		resp[i] = ac
		i++
	}
	body, _ := json.Marshal(resp)
	go lib.SetRangeCacheFromS3(s3client, key, "GetUserActiveDays", body)
	return resp
}
