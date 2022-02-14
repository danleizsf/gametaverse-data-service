package main

import (
	"fmt"
	"gametaverse-data-service/schema"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserRetentionRate(fromTimeObj time.Time, toTimeObj time.Time) float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)
	fromDateTimestamp := fromTimeObj.Unix()
	toDateTimestamp := toTimeObj.Unix()

	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.DailyTransferBucketName),
			Key:    aws.String(fmt.Sprintf("%d-in-game-token-transfers-with-timestamp.csv", fromDateTimestamp)),
		}

	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}
	bodyString := string(body)
	fromDateTransfers := ConvertCsvStringToTransferStructs(bodyString)

	requestInput =
		&s3.GetObjectInput{
			Bucket: aws.String(schema.DailyTransferBucketName),
			Key:    aws.String(fmt.Sprintf("%d-in-game-token-transfers-with-timestamp.csv", toDateTimestamp)),
		}

	result, err = svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err = ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}
	bodyString = string(body)
	toDateTransfers := ConvertCsvStringToTransferStructs(bodyString)

	fromDateActiveUsers := getActiveUsersFromTransfers(fromDateTransfers)
	toDateActiveUsers := getActiveUsersFromTransfers(toDateTransfers)
	retentionedUsers := map[string]bool{}
	for fromDateUser := range fromDateActiveUsers {
		if _, ok := toDateActiveUsers[fromDateUser]; ok {
			retentionedUsers[fromDateUser] = true
		}
	}
	return float64(len(retentionedUsers)) / float64(len(fromDateActiveUsers))
}
