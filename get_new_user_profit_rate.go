package main

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserProfitableRate(fromTimeObj time.Time, toTimeObj time.Time) AllUserRoiDetails {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	newUsers := getNewUsers(fromTimeObj, toTimeObj, *svc)

	totalTransfers := GetTransfers(fromTimeObj, toTimeObj)
	//for _, item := range resp.Contents {
	//	log.Printf("file name: %s\n", *item.Key)
	//	timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
	//	timeObj := time.Unix(timestamp, 0)
	//	if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
	//		continue
	//	}

	//	requestInput :=
	//		&s3.GetObjectInput{
	//			Bucket: aws.String(dailyTransferBucketName),
	//			Key:    aws.String(*item.Key),
	//		}
	//	result, err := svc.GetObject(requestInput)
	//	if err != nil {
	//		exitErrorf("Unable to get object, %v", err)
	//	}
	//	body, err := ioutil.ReadAll(result.Body)
	//	if err != nil {
	//		exitErrorf("Unable to get body, %v", err)
	//	}
	//	bodyString := string(body)
	//	//transactions := converCsvStringToTransactionStructs(bodyString)
	//	transfers := ConvertCsvStringToTransferStructs(bodyString)
	//	log.Printf("transfer num: %d", len(transfers))
	//	//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
	//	totalTransfers = append(totalTransfers, transfers...)
	//}
	perNewUserRoiDetail := map[string]*UserRoiDetail{}

	priceHistory := getPriceHistory("sea", fromTimeObj, toTimeObj, *svc)
	priceHisoryMap := map[int64]float64{}
	layout := "2006-01-02"
	for _, price := range priceHistory.Prices {
		timeObj, _ := time.Parse(layout, price.Date)
		priceHisoryMap[timeObj.Unix()] = price.Price
	}
	for _, transfer := range totalTransfers {
		//if transfer.FromAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" && transfer.ToAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" {
		//	continue
		//}

		//log.Printf("user %s transfer %v", "0xfff5de86577b3f778ac6cc236384ed6db1825bff", transfer)
		if joinedTimestamp, ok := newUsers[transfer.FromAddress]; ok {
			dateTimestamp := (joinedTimestamp / int64(dayInSec)) * int64(dayInSec)
			valueUsd := (transfer.Value / float64(seaTokenUnit)) * priceHisoryMap[dateTimestamp]
			valueToken := transfer.Value / float64(seaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.FromAddress]; ok {
				userRoiDetails.TotalProfitUsd -= valueUsd
				userRoiDetails.TotalSpendingUsd += valueUsd
				userRoiDetails.TotalProfitToken -= valueToken
				userRoiDetails.TotalSpendingToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.FromAddress] = &UserRoiDetail{
					UserAddress:        transfer.FromAddress,
					JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   valueUsd,
					TotalProfitUsd:     -valueUsd,
					TotalSpendingToken: valueToken,
					TotalProfitToken:   -valueToken,
				}
			}
		}
		if joinedTimestamp, ok := newUsers[transfer.ToAddress]; ok {
			dateTimestamp := (joinedTimestamp / int64(dayInSec)) * int64(dayInSec)
			valueUsd := (transfer.Value / float64(seaTokenUnit)) * priceHisoryMap[dateTimestamp]
			valueToken := transfer.Value / float64(seaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.ToAddress]; ok {
				userRoiDetails.TotalProfitUsd += valueUsd
				userRoiDetails.TotalProfitToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.ToAddress] = &UserRoiDetail{
					UserAddress:        transfer.ToAddress,
					JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   0,
					TotalProfitUsd:     valueUsd,
					TotalSpendingToken: 0,
					TotalProfitToken:   valueToken,
				}
			}
		}
	}
	userRoiDetails := make([]UserRoiDetail, len(perNewUserRoiDetail))
	profitableUserCount := 0
	idx := 0
	for _, userRoiDetail := range perNewUserRoiDetail {
		userRoiDetails[idx] = *userRoiDetail
		idx += 1
		if userRoiDetail.TotalProfitUsd > 0 {
			profitableUserCount += 1
		}
	}

	//log.Printf("priceHistory: %v", priceHistory)
	//return AllUserRoiDetails{}
	return AllUserRoiDetails{
		UserRoiDetails:        userRoiDetails,
		OverallProfitableRate: float64(profitableUserCount) / float64(len(perNewUserRoiDetail)),
	}
}
