package main

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserProfitDistribution(userAddress string, fromTimeObj time.Time, toTimeObj time.Time) UserRoiDetail {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	totalTransfers := make([]Transfer, 0)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}

	userRoiDetail := UserRoiDetail{}
	for _, transfer := range totalTransfers {
		if transfer.FromAddress == userAddress {
			userRoiDetail.TotalSpendingToken += transfer.Value / float64(seaTokenUnit)
			userRoiDetail.TotalProfitToken -= transfer.Value / float64(seaTokenUnit)
		} else if transfer.ToAddress == userAddress {
			userRoiDetail.TotalProfitToken += transfer.Value / float64(seaTokenUnit)
		}
	}

	userRoiDetail.UserAddress = userAddress
	return userRoiDetail
}
