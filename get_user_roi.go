package main

import (
	"gametaverse-data-service/schema"
	"math"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserRoi(fromTimeObjs time.Time, toTimeObj time.Time) []schema.UserRoiDetail {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	totalTransfers := GetTransfers(fromTimeObjs, toTimeObj)
	targetUsers := getNewUsers(fromTimeObjs, toTimeObj, *svc)

	targetUserTransfers := map[string][]schema.Transfer{}

	for _, transfer := range totalTransfers {
		if _, ok := targetUsers[transfer.FromAddress]; ok {
			if _, ok := targetUserTransfers[transfer.FromAddress]; ok {
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			} else {
				targetUserTransfers[transfer.FromAddress] = make([]schema.Transfer, 0)
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			}
		}
		if _, ok := targetUsers[transfer.ToAddress]; ok {
			if _, ok := targetUserTransfers[transfer.ToAddress]; ok {
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			} else {
				targetUserTransfers[transfer.ToAddress] = make([]schema.Transfer, 0)
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			}
		}
	}
	starSharksMysteriousBoxTransfers := getMysteriousBoxTransfers(fromTimeObjs, toTimeObj, *svc)
	for _, transfer := range starSharksMysteriousBoxTransfers {
		targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
	}

	for userAddress, transfers := range targetUserTransfers {
		sort.Slice(targetUserTransfers[userAddress], func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
	}

	userRois := make([]schema.UserRoiDetail, 0)
	for userAddress, transfers := range targetUserTransfers {
		value := 0
		transferIdx := -1
		for _, transfer := range transfers {
			if transfer.FromAddress == userAddress {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("spend %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value -= int(transfer.Value / 1000000000000000000)
			} else if transfer.ToAddress == userAddress {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("earn %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value += int(transfer.Value / 1000000000000000000)
			}
			transferIdx += 1
			if value > 0 {
				break
			}
		}

		if value < 0 {
			continue
		}
		payerType := GetPayerType(transfers)

		initialTransferTimeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		profitTransferTimeObj := time.Unix(int64(transfers[transferIdx].Timestamp), 0)
		profitableDays := int64(math.Ceil(profitTransferTimeObj.Sub(initialTransferTimeObj).Hours() / 24))
		userRois = append(userRois, schema.UserRoiDetail{
			ProfitableDays: profitableDays,
			UserType:       payerType,
		})
	}

	return userRois
}
