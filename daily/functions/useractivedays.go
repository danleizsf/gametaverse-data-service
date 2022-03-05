package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserActiveDays(s3client *s3.S3, timestampA int64, timestampB int64, limit int64) []schema.UserActivity {
	useractions := lib.GetUserActionsRangeAsync(s3client, timestampA, timestampB)
	perUserActivities := make(map[string]schema.UserActivity, len(useractions))

	for userAddress, actions := range useractions {
		firstDate := actions[0].Time
		lastDate := actions[len(actions)-1].Time
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
	resp := make([]schema.UserActivity, len(perUserActivities))
	i := 0
	for _, ac := range perUserActivities {
		resp[i] = ac
		i++
	}

	return resp
}
