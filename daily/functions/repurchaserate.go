package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserRepurchaseRate(s3client *s3.S3, timestampA int64, timestampB int64) float64 {
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)
	length := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		length++
	}
	useractions := make(map[string][]schema.UserAction, 0)
	var wg sync.WaitGroup
	wg.Add(length)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format("2006-01-02")
			ua := lib.GetUserActions(s3client, date)
			for user, actions := range ua {
				actionsWithDate := make([]schema.UserAction, len(actions))
				for i, a := range actions {
					actionsWithDate[i] = schema.UserAction{
						Value:  a.Value,
						Date:   date,
						Action: a.Action,
					}
				}

				if existingActions, exists := useractions[user]; exists {
					useractions[user] = append(existingActions, actionsWithDate...)
				} else {
					useractions[user] = actionsWithDate
				}
			}
		}(s3client, d)
	}
	wg.Wait()
	var repurchaseUserCount int
	for userAddress, actions := range useractions {
		if actions[len(actions)-1].Date != actions[0].Date {
			repurchaseUserCount++
			log.Printf("repurchase user, %s, %+v", userAddress, actions)
		}
	}

	return float64(repurchaseUserCount) / float64(len(useractions))
}
