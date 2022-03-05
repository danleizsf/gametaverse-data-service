package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserRoi(s3client *s3.S3, start time.Time, end time.Time) []schema.UserRoiDetail {
	length := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		length++
	}
	concurrentNewUser := make([][]string, length+1)
	concurrentUserActions := make([]map[string][]schema.UserAction, length+1)

	var wg sync.WaitGroup
	wg.Add(length)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format(schema.DateFormat)
			s := lib.GetSummary(s3client, date)
			uas := lib.GetUserActions(s3client, date)
			concurrentNewUser[i] = s.NewUser
			concurrentUserActions[i] = uas
		}(i, s3client, d)
		i++
	}
	wg.Wait()

	newUsers := map[string]bool{}
	for _, dailyNewUsers := range concurrentNewUser {
		for _, newUser := range dailyNewUsers {
			newUsers[newUser] = true
		}
	}
	newUserActions := make(map[string][]schema.UserAction, len(newUsers))
	for _, dailyUserActions := range concurrentUserActions {
		for user, actions := range dailyUserActions {
			if !newUsers[user] {
				continue
			}

			if ua, exists := newUserActions[user]; exists {
				newUserActions[user] = append(ua, actions...)
			} else {
				newUserActions[user] = actions
			}
		}
	}

	userRois := make([]schema.UserRoiDetail, 0)
	for _, actions := range newUserActions {
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
		profitableDays := int64(math.Ceil(actions[profitableIndex].Time.Sub(actions[0].Time).Hours() / 24))
		userRois = append(userRois, schema.UserRoiDetail{
			ProfitableDays: profitableDays,
			UserType:       payerType,
		})
	}

	return userRois

}
