package main

import (
	"gametaverse-data-service/schema"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserProfitableRate(fromTimeObj time.Time, toTimeObj time.Time, forDebug bool) schema.AllUserRoiDetails {
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
	perNewUserRoiDetail := map[string]*schema.UserRoiDetail{}

	priceHistory := getPriceHistory("sea", fromTimeObj, toTimeObj, *svc)
	priceHisoryMap := map[int64]float64{}
	layout := "2006-01-02"
	for _, price := range priceHistory.Prices {
		timeObj, _ := time.Parse(layout, price.Date)
		priceHisoryMap[timeObj.Unix()] = price.Price
	}
	payerTypes := GetPayerTypes(totalTransfers)
	for _, transfer := range totalTransfers {
		//if transfer.FromAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" && transfer.ToAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" {
		//	continue
		//}

		//log.Printf("user %s transfer %v", "0xfff5de86577b3f778ac6cc236384ed6db1825bff", transfer)
		if joinedTimestamp, ok := newUsers[transfer.FromAddress]; ok {
			dateTimestamp := (int64(transfer.Timestamp) / int64(schema.DayInSec)) * int64(schema.DayInSec)
			valueUsd := (transfer.Value / float64(schema.SeaTokenUnit)) * priceHisoryMap[dateTimestamp]
			valueToken := transfer.Value / float64(schema.SeaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.FromAddress]; ok {
				userRoiDetails.TotalProfitUsd -= valueUsd
				userRoiDetails.TotalSpendingUsd += valueUsd
				userRoiDetails.TotalProfitToken -= valueToken
				userRoiDetails.TotalSpendingToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.FromAddress] = &schema.UserRoiDetail{
					UserAddress:        transfer.FromAddress,
					JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   valueUsd,
					TotalProfitUsd:     -valueUsd,
					TotalSpendingToken: valueToken,
					TotalProfitToken:   -valueToken,
					UserType:           payerTypes[transfer.FromAddress],
				}
			}
		}
		if joinedTimestamp, ok := newUsers[transfer.ToAddress]; ok {
			dateTimestamp := (int64(transfer.Timestamp) / int64(schema.DayInSec)) * int64(schema.DayInSec)
			valueUsd := (transfer.Value / float64(schema.SeaTokenUnit)) * priceHisoryMap[dateTimestamp]
			valueToken := transfer.Value / float64(schema.SeaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.ToAddress]; ok {
				userRoiDetails.TotalProfitUsd += valueUsd
				userRoiDetails.TotalProfitToken += valueToken
			} else {
				perNewUserRoiDetail[transfer.ToAddress] = &schema.UserRoiDetail{
					UserAddress:        transfer.ToAddress,
					JoinDateTimestamp:  joinedTimestamp,
					TotalSpendingUsd:   0,
					TotalProfitUsd:     valueUsd,
					TotalSpendingToken: 0,
					TotalProfitToken:   valueToken,
					UserType:           payerTypes[transfer.ToAddress],
				}
			}
		}
	}

	starSharksMysteriousBoxTransfers := getMysteriousBoxTransfers(fromTimeObj, toTimeObj, *svc)
	for _, transfer := range starSharksMysteriousBoxTransfers {
		dateTimestamp := (int64(transfer.Timestamp) / int64(schema.DayInSec)) * int64(schema.DayInSec)
		address := transfer.FromAddress
		valueToken := transfer.Value / float64(schema.SeaTokenUnit)
		valueUsd := valueToken * priceHisoryMap[dateTimestamp]
		if _, ok := perNewUserRoiDetail[address]; ok {
			perNewUserRoiDetail[address].TotalProfitToken -= valueToken
			perNewUserRoiDetail[address].TotalProfitUsd -= valueUsd
			perNewUserRoiDetail[address].TotalSpendingToken += valueToken
			perNewUserRoiDetail[address].TotalSpendingUsd += valueUsd
		}
	}
	userRoiDetails := make([]schema.UserRoiDetail, len(perNewUserRoiDetail))
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
	response := schema.AllUserRoiDetails{
		OverallProfitableRate: float64(profitableUserCount) / float64(len(perNewUserRoiDetail)),
	}
	if forDebug {
		response.UserRoiDetails = userRoiDetails
	}
	return response
}
