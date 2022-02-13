package main

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserProfitDistribution(userAddresses map[string]bool) []UserRoiDetail {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	fromTimeObj := starSharksStartingDate
	toTimeObj := time.Now()
	totalTransfers := GetTransfers(fromTimeObj, toTimeObj)

	priceHistory := getPriceHistory("sea", fromTimeObj, toTimeObj, *svc)
	priceHisoryMap := map[int64]float64{}
	layout := "2006-01-02"
	for _, price := range priceHistory.Prices {
		timeObj, _ := time.Parse(layout, price.Date)
		priceHisoryMap[timeObj.Unix()] = price.Price
	}
	log.Printf("priceHistoryMap %v", priceHisoryMap)
	perNewUserRoiDetail := map[string]*UserRoiDetail{}
	for _, transfer := range totalTransfers {
		if _, ok := userAddresses[transfer.FromAddress]; ok {
			valueUsd := (transfer.Value / float64(seaTokenUnit)) * priceHisoryMap[int64(transfer.Timestamp)]
			valueToken := transfer.Value / float64(seaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.FromAddress]; ok {
				userRoiDetails.TotalProfitUsd -= valueUsd
				userRoiDetails.TotalSpendingUsd += valueUsd
				userRoiDetails.TotalProfitToken -= valueToken
				userRoiDetails.TotalSpendingToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.FromAddress] = &UserRoiDetail{
					UserAddress: transfer.FromAddress,
					//JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   valueUsd,
					TotalProfitUsd:     -valueUsd,
					TotalSpendingToken: valueToken,
					TotalProfitToken:   -valueToken,
				}
			}
		}
		if _, ok := userAddresses[transfer.ToAddress]; ok {
			valueUsd := (transfer.Value / float64(seaTokenUnit)) * priceHisoryMap[int64(transfer.Timestamp)]
			valueToken := transfer.Value / float64(seaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.ToAddress]; ok {
				userRoiDetails.TotalProfitUsd += valueUsd
				userRoiDetails.TotalProfitToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.ToAddress] = &UserRoiDetail{
					UserAddress: transfer.ToAddress,
					//JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   0,
					TotalProfitUsd:     valueUsd,
					TotalSpendingToken: 0,
					TotalProfitToken:   valueToken,
				}
			}
		}
	}

	userRoiDetails := make([]UserRoiDetail, len(perNewUserRoiDetail))
	idx := 0
	for _, userRoiDetail := range perNewUserRoiDetail {
		userRoiDetails[idx] = *userRoiDetail
		idx += 1
	}
	return userRoiDetails
}