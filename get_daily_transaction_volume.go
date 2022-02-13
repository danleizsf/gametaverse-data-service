package main

import (
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetGameDailyTransactionVolumes(targetTimeObjs []time.Time) []DailyTransactionVolume {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	dailyTransactionVolume := make(map[int64]UserTransactionVolume)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if !isEligibleToProcess(timeObj, targetTimeObjs) {
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
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		dailyTransactionVolume[int64(dateTimestamp)] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
	}

	result := make([]DailyTransactionVolume, len(dailyTransactionVolume))
	idx := 0
	for dateTimestamp, transactionVolume := range dailyTransactionVolume {
		result[idx] = DailyTransactionVolume{
			DateTimestamp:          dateTimestamp,
			TotalTransactionVolume: transactionVolume,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DateTimestamp < result[j].DateTimestamp
	})
	return result
}
