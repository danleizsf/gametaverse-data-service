package main

import (
	"gametaverse-data-service/schema"
	"io/ioutil"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	//daus := make(map[int64]schema.Dau)

	bucketName := "gametaverse-bucket"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	concurrencyCount := 8
	tempTotalDaus := make([][]schema.Dau, concurrencyCount)
	s3FileList := make([]*string, 0)
	s3FileChuncks := make([][]*string, concurrencyCount)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}
		s3FileList = append(s3FileList, item.Key)
	}

	log.Printf("s3FileList: %v", s3FileList)
	chunckSize := int(math.Ceil(float64(len(s3FileList)) / float64(concurrencyCount)))
	for chunckIdx := 0; chunckIdx < concurrencyCount; chunckIdx++ {
		chunck := make([]*string, 0)
		for j := 0; j < chunckSize; j++ {
			fileIdx := chunckIdx*chunckSize + j
			if fileIdx >= len(s3FileList) {
				break
			}
			chunck = append(chunck, s3FileList[fileIdx])
		}
		log.Printf("chuckIdx: %d, chunck: %v", chunckIdx, chunck)
		s3FileChuncks[chunckIdx] = chunck
	}
	log.Printf("s3FileChuncks: %v", s3FileChuncks)

	var wg sync.WaitGroup
	wg.Add(len(s3FileChuncks))
	for i, fileNameChunck := range s3FileChuncks {
		go func(i int, chunck []*string) {
			defer wg.Done()
			chunckSvc := s3.New(sess)
			log.Printf("start chunck %d, size %d", i, len(chunck))
			dauChunck := make([]schema.Dau, 0)
			for _, fileName := range chunck {

				timestamp, _ := strconv.ParseInt(strings.Split(*fileName, "-")[0], 10, 64)
				timeObj := time.Unix(timestamp, 0)
				if fileName == nil {
					exitErrorf("to delete")
				}
				requestInput := &s3.GetObjectInput{
					Bucket: aws.String(schema.DailyTransferBucketName),
					Key:    aws.String(*fileName),
				}
				result, err := chunckSvc.GetObject(requestInput)
				if err != nil {
					exitErrorf("Unable to get object, %v", err)
				}
				body, err := ioutil.ReadAll(result.Body)
				if err != nil || body == nil {
					exitErrorf("Unable to get body, %v", err)
				}
				bodyString := string(body)
				transfers := ConvertCsvStringToTransferStructs(bodyString)
				//transferChunck = append(transferChunck, transfers...)

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
				dauChunck = append(dauChunck, schema.Dau{
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
				})
			}
			log.Printf("end chunck %d", i)
			tempTotalDaus[i] = dauChunck
		}(i, fileNameChunck)
	}
	wg.Wait()

	//for _, item := range resp.Contents {
	//	log.Printf("file name: %s\n", *item.Key)
	//	timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
	//	timeObj := time.Unix(timestamp, 0)
	//	if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
	//		continue
	//	}
	//	log.Printf("filtered time: %v", timeObj)

	//	requestInput :=
	//		&s3.GetObjectInput{
	//			Bucket: aws.String(bucketName),
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
	//	//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
	//	perPayerTransfers := getPerPayerTransfers(transfers)
	//	//perUserTransfers := getActiveUsersFromTransfers(transfers)
	//	totalPerPayerType := GetPerPayerType(perPayerTransfers)
	//	totalRenterCount, totalPurchaserCount := 0, 0
	//	for _, payerType := range totalPerPayerType {
	//		if payerType == schema.Rentee {
	//			totalRenterCount += 1
	//		} else if payerType == schema.Purchaser {
	//			totalPurchaserCount += 1
	//		}
	//	}

	//	newUsers := getNewUsers(timeObj, time.Unix(timestamp+int64(schema.DayInSec), 0), *svc)
	//	perNewPayerTransfers := map[string][]schema.Transfer{}
	//	for payerAddress, transfers := range perPayerTransfers {
	//		if _, ok := newUsers[payerAddress]; ok {
	//			perNewPayerTransfers[payerAddress] = transfers
	//		}
	//	}
	//	perNewPayerType := GetPerPayerType(perNewPayerTransfers)
	//	newRenterCount, newPurchaserCount := 0, 0
	//	for _, payerType := range perNewPayerType {
	//		if payerType == schema.Rentee {
	//			newRenterCount += 1
	//		} else if payerType == schema.Purchaser {
	//			newPurchaserCount += 1
	//		}
	//	}
	//	daus[timestamp] = schema.Dau{
	//		DateTimestamp: timestamp,
	//		TotalActiveUsers: schema.ActiveUserCount{
	//			TotalUserCount: int64(len(getActiveUsersFromTransfers(transfers))),
	//			PayerCount: schema.PayerCount{
	//				RenteeCount:    int64(totalRenterCount),
	//				PurchaserCount: int64(totalPurchaserCount),
	//			},
	//		},
	//		NewActiveUsers: schema.ActiveUserCount{
	//			TotalUserCount: int64(len(newUsers)),
	//			PayerCount: schema.PayerCount{
	//				RenteeCount:    int64(newRenterCount),
	//				PurchaserCount: int64(newPurchaserCount),
	//			},
	//		},
	//	}
	//}
	//result := make([]schema.Dau, len(daus))
	//idx := 0
	//for _, dau := range daus {
	//	result[idx] = dau
	//	idx += 1
	//}
	//sort.Slice(result, func(i, j int) bool {
	//	return result[i].DateTimestamp < result[j].DateTimestamp
	//})
	//return result

	totalDaus := make([]schema.Dau, 0)
	for _, dauChunck := range tempTotalDaus {
		totalDaus = append(totalDaus, dauChunck...)
	}
	sort.Slice(totalDaus, func(i, j int) bool {
		return totalDaus[i].DateTimestamp < totalDaus[j].DateTimestamp
	})
	return totalDaus
}
