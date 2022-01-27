package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"
)

func getGameData() (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")
	daus := make(map[string]int)
	dailyTransactionVolume := make(map[string]uint64)

	for _, bucket := range result.Buckets {
		log.Printf("* %s created on %s\n", aws.StringValue(bucket.Name), aws.TimeValue(bucket.CreationDate))

		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(*bucket.Name)})
		if err != nil {
			exitErrorf("Unable to list object, %v", err)
		}

		for _, item := range resp.Contents {
			log.Printf("file name: %s\n", *item.Key)
			requestInput :=
				&s3.GetObjectInput{
					Bucket: aws.String(*bucket.Name),
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
			bodyString := fmt.Sprintf("%s", body)
			//transactions := converCsvStringToTransactionStructs(bodyString)
			transfers := converCsvStringToTransferStructs(bodyString)
			log.Printf("transfer num: %d", len(transfers))
			dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
			//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
			dateObj := time.Unix(int64(dateTimestamp), 0).UTC()
			dateFormattedString := fmt.Sprintf("%d-%d-%d", dateObj.Year(), dateObj.Month(), dateObj.Day())
			//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
			daus[dateFormattedString] = getActiveUserNumFromTransfers(transfers, int64(dateTimestamp))
			dailyTransactionVolume[dateFormattedString] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
		}
	}
	return fmt.Sprintf("{starsharks: {dau: %v, dailyTransactionVolume: %v SEA Token}}", daus, dailyTransactionVolume), nil
}

func getUserData() (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")
	daus := make(map[string]int)
	dailyTransactionVolume := make(map[string]uint64)

	for _, bucket := range result.Buckets {
		log.Printf("* %s created on %s\n", aws.StringValue(bucket.Name), aws.TimeValue(bucket.CreationDate))

		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(*bucket.Name)})
		if err != nil {
			exitErrorf("Unable to list object, %v", err)
		}

		for _, item := range resp.Contents {
			log.Printf("file name: %s\n", *item.Key)
			requestInput :=
				&s3.GetObjectInput{
					Bucket: aws.String(*bucket.Name),
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
			bodyString := fmt.Sprintf("%s", body)
			//transactions := converCsvStringToTransactionStructs(bodyString)
			transfers := converCsvStringToTransferStructs(bodyString)
			log.Printf("transfer num: %d", len(transfers))
			dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
			//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
			dateObj := time.Unix(int64(dateTimestamp), 0).UTC()
			dateFormattedString := fmt.Sprintf("%d-%d-%d", dateObj.Year(), dateObj.Month(), dateObj.Day())
			//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
			daus[dateFormattedString] = getActiveUserNumFromTransfers(transfers, int64(dateTimestamp))
			dailyTransactionVolume[dateFormattedString] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
		}
	}
	return fmt.Sprintf("{starsharks: {dau: %v, dailyTransactionVolume: %v SEA Token}}", daus, dailyTransactionVolume), nil
}

func process(ctx context.Context, req events.APIGatewayProxyRequest) (string, error) {
	return getGameData()
}
func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(process)
}
