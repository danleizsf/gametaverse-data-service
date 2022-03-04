package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserRepurchaseRate(s3client *s3.S3, timestampA int64, timestampB int64) float64 {
	// start := lib.GetDate(timestampA)
	// end := lib.GetDate(timestampB)
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)
	log.Print(start, end)
	useractions := make(map[string][]schema.UserAction, 0)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		date := d.Format("2006-01-02")
		log.Print(date)
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
	}
	var repurchaseUserCount int
	for userAddress, actions := range useractions {
		if actions[len(actions)-1].Date != actions[0].Date {
			repurchaseUserCount++
			log.Printf("repurchase user, %s, %+v", userAddress, actions)
		}
	}

	return float64(repurchaseUserCount) / float64(len(useractions))
}
