package main

import (
	"gametaverse-data-service/schema"
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

func GetGameDaus(fromTimeObj time.Time, toTimeObj time.Time) []schema.Dau {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	daus := make(map[int64]schema.Dau)

	bucketName := "gametaverse-bucket"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
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
		log.Printf("filtered time: %v", timeObj)

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
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
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
		perPayerTransfers := getPerPayerTransfers(transfers)
		//perUserTransfers := getActiveUsersFromTransfers(transfers)
		totalPerPayerType := GetPerPayerType(perPayerTransfers)
		totalRenterCount, totalPurchaserCount := 0, 0
		for _, payerType := range totalPerPayerType {
			if payerType == schema.Rentee {
				totalRenterCount += 1
			} else if payerType == schema.Purchaser {
				totalPurchaserCount += 1
			}
		}

		newUsers := getNewUsers(timeObj, time.Unix(timestamp+int64(schema.DayInSec), 0), *svc)
		perNewPayerTransfers := map[string][]schema.Transfer{}
		for payerAddress, transfers := range perPayerTransfers {
			if _, ok := newUsers[payerAddress]; ok {
				perNewPayerTransfers[payerAddress] = transfers
			}
		}
		perNewPayerType := GetPerPayerType(perNewPayerTransfers)
		newRenterCount, newPurchaserCount := 0, 0
		for _, payerType := range perNewPayerType {
			if payerType == schema.Rentee {
				newRenterCount += 1
			} else if payerType == schema.Purchaser {
				newPurchaserCount += 1
			}
		}
		daus[timestamp] = schema.Dau{
			DateTimestamp: timestamp,
			TotalActiveUsers: schema.ActiveUserCount{
				TotalUserCount: int64(len(getActiveUsersFromTransfers(transfers))),
				PayerCount: schema.PayerCount{
					RenteeCount:    int64(totalRenterCount),
					PurchaserCount: int64(totalPurchaserCount),
				},
			},
			NewActiveUsers: schema.ActiveUserCount{
				TotalUserCount: int64(len(newUsers)),
				PayerCount: schema.PayerCount{
					RenteeCount:    int64(newRenterCount),
					PurchaserCount: int64(newPurchaserCount),
				},
			},
		}
	}
	result := make([]schema.Dau, len(daus))
	idx := 0
	for _, dau := range daus {
		result[idx] = dau
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DateTimestamp < result[j].DateTimestamp
	})
	return result
}
