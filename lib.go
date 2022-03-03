package main

import (
	"encoding/json"
	"gametaverse-data-service/schema"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetTransfers(fromTimeObj time.Time, toTimeObj time.Time) []schema.Transfer {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(schema.DailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	concurrencyCount := 20
	tempTotalTransfers := make([][]schema.Transfer, concurrencyCount)
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
			transferChunck := make([]schema.Transfer, 0)
			for _, fileName := range chunck {

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
				transferChunck = append(transferChunck, transfers...)
			}
			log.Printf("end chunck %d", i)
			tempTotalTransfers[i] = transferChunck
		}(i, fileNameChunck)
	}
	wg.Wait()
	log.Printf("hello")
	totalTransfers := make([]schema.Transfer, 0)
	for _, transferChunk := range tempTotalTransfers {
		totalTransfers = append(totalTransfers, transferChunk...)
	}
	return totalTransfers
}

func GetPerPayerType(perPayerTransfers map[string][]schema.Transfer) map[string]schema.PayerType {
	perPayerType := map[string]schema.PayerType{}
	for payerAddress, transfers := range perPayerTransfers {
		totalRentingValue := float64(0)
		totalInvestingValue := float64(0)
		for _, transfer := range transfers {
			if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {
				totalInvestingValue += transfer.Value / float64(schema.DayInSec)
			} else if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
				totalRentingValue += transfer.Value / float64(schema.DayInSec)
			}
		}
		if totalInvestingValue > totalRentingValue {
			perPayerType[payerAddress] = schema.Purchaser
		} else {
			perPayerType[payerAddress] = schema.Rentee
		}
	}
	return perPayerType
}

func ConvertCsvStringToTransferStructs(csvString string) []schema.Transfer {
	lines := strings.Split(csvString, "\n")
	transfers := make([]schema.Transfer, 0)
	count := 0
	//log.Printf("enterred converCsvStringToTransferStructs, content len: %d", len(lines))
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 8 {
			continue
		}
		token_address := fields[0]
		if token_address != "0x26193c7fa4354ae49ec53ea2cebc513dc39a10aa" {
			continue
		}
		count += 1
		timestamp, _ := strconv.Atoi(fields[7])
		blockNumber, _ := strconv.Atoi(fields[6])
		value, _ := strconv.ParseFloat(fields[3], 64)
		logIndex, _ := strconv.Atoi(fields[5])
		transfers = append(transfers, schema.Transfer{
			TokenAddress:    fields[0],
			FromAddress:     fields[1],
			ToAddress:       fields[2],
			Value:           value,
			TransactionHash: fields[4],
			LogIndex:        logIndex,
			BlockNumber:     blockNumber,
			Timestamp:       timestamp,
			ContractAddress: fields[8],
		})
	}
	//log.Printf("left converCsvStringToTransferStructs, content len: %d", len(lines))
	return transfers
}

func getActiveUsersFromTransfers(transfers []schema.Transfer) map[string]bool {
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transfer := range transfers {
		count += 1
		uniqueAddresses[transfer.FromAddress] = true
		uniqueAddresses[transfer.ToAddress] = true
	}
	return uniqueAddresses
}

//func getUserTransactionVolume(address string, transfers []Transfer) float64 {
//	transactionVolume := float64(0)
//	for _, transfer := range transfers {
//		if transfer.FromAddress == address || transfer.ToAddress == address {
//			transactionVolume += transfer.Value
//			log.Printf("address: %s, transactionHash: %s, value: %v", address, transfer.TransactionHash, transfer.Value)
//		}
//	}
//	return transactionVolume / 1000000000000000000
//}

func getTransactionVolumeFromTransfers(transfers []schema.Transfer, timestamp int64) schema.UserTransactionVolume {
	renterTransactionVolume, purchaserTransactionVolume, withdrawerTransactionVolume := int64(0), int64(0), int64(0)
	for _, transfer := range transfers {
		if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
			renterTransactionVolume += int64(transfer.Value / float64(schema.SeaTokenUnit))
		} else if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {
			purchaserTransactionVolume += int64(transfer.Value / float64(schema.SeaTokenUnit))
		} else if transfer.ContractAddress == schema.StarSharksWithdrawContractAddresses {
			withdrawerTransactionVolume += int64(transfer.Value / float64(schema.SeaTokenUnit))
		}
	}
	return schema.UserTransactionVolume{
		RenterTransactionVolume:     renterTransactionVolume,
		PurchaserTransactionVolume:  purchaserTransactionVolume,
		WithdrawerTransactionVolume: withdrawerTransactionVolume,
	}
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

//func getUserData(address string) (string, error) {
//	sess, _ := session.NewSession(&aws.Config{
//		Region: aws.String("us-west-1"),
//	})
//
//	svc := s3.New(sess)
//
//	dailyTransactionVolume := make(map[string]float64)
//
//	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
//	if err != nil {
//		exitErrorf("Unable to list object, %v", err)
//	}
//
//	for _, item := range resp.Contents {
//		log.Printf("file name: %s\n", *item.Key)
//		requestInput :=
//			&s3.GetObjectInput{
//				Bucket: aws.String(dailyTransferBucketName),
//				Key:    aws.String(*item.Key),
//			}
//		result, err := svc.GetObject(requestInput)
//		if err != nil {
//			exitErrorf("Unable to get object, %v", err)
//		}
//		body, err := ioutil.ReadAll(result.Body)
//		if err != nil {
//			exitErrorf("Unable to get body, %v", err)
//		}
//		bodyString := string(body)
//		//transactions := converCsvStringToTransactionStructs(bodyString)
//		transfers := ConvertCsvStringToTransferStructs(bodyString)
//		log.Printf("transfer num: %d", len(transfers))
//		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
//		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
//		dateObj := time.Unix(int64(dateTimestamp), 0).UTC()
//		dateFormattedString := fmt.Sprintf("%d-%d-%d", dateObj.Year(), dateObj.Month(), dateObj.Day())
//		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
//		dailyTransactionVolume[dateFormattedString] = getUserTransactionVolume(address, transfers)
//	}
//	return fmt.Sprintf("{starsharks: {dailyTransactionVolume: %v SEA Token}}", dailyTransactionVolume), nil
//}

func getPerUserSpending(transfers []schema.Transfer) map[string]int64 {
	perUserSpending := make(map[string]int64)
	for _, transfer := range transfers {
		if _, ok := schema.StarSharksGameWalletAddresses[transfer.FromAddress]; ok {
			continue
		}
		if spending, ok := perUserSpending[transfer.FromAddress]; ok {
			perUserSpending[transfer.FromAddress] = spending + int64(transfer.Value/1000000000000000000)
		} else {
			perUserSpending[transfer.FromAddress] = int64(transfer.Value / 1000000000000000000)
		}
	}
	return perUserSpending
}

func generateValueDistribution(perUserValue map[string]int64) []schema.ValueFrequencyPercentage {
	valueDistribution := make(map[int64]int64)
	totalFrequency := int64(0)
	for _, value := range perUserValue {
		valueDistribution[value] += 1
		totalFrequency += 1
	}
	valuePercentageDistribution := make(map[int64]float64)
	for value, frequency := range valueDistribution {
		valuePercentageDistribution[value] = float64(frequency) / float64(totalFrequency)
	}
	result := make([]schema.ValueFrequencyPercentage, len(valuePercentageDistribution))
	idx := 0
	for value, percentage := range valuePercentageDistribution {
		result[idx] = schema.ValueFrequencyPercentage{
			Value:               value,
			FrequencyPercentage: percentage,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Value < result[j].Value
	})
	return result
}

//func isEligibleToProcess(timeObj time.Time, targetTimeObjs []time.Time) bool {
//	eligibleToProcess := false
//	for _, targetTimeObj := range targetTimeObjs {
//		log.Printf("targetTime: %v, time: %v", targetTimeObj, timeObj)
//		if targetTimeObj.Year() == timeObj.Year() && targetTimeObj.Month() == timeObj.Month() && targetTimeObj.Day() == timeObj.Day() {
//			eligibleToProcess = true
//			break
//		}
//	}
//	return eligibleToProcess
//}
//
//func generateTimeObjs(input schema.Input) []time.Time {
//	times := make([]time.Time, 0)
//	for _, param := range input.Params {
//		if param.Timestamp != 0 {
//			times = append(times, time.Unix(param.Timestamp, 0))
//		}
//	}
//	return times
//}
//
//func generateRoiDistribution(perUserRoiInDays map[string]int64) []schema.ValueFrequencyPercentage {
//	RoiDayDistribution := make(map[int64]int64)
//	totalCount := float64(0)
//	for _, days := range perUserRoiInDays {
//		if days < 1 {
//			continue
//		}
//		RoiDayDistribution[days] += 1
//		totalCount += 1
//	}
//	daysPercentageDistribution := make(map[int64]float64)
//	for days, count := range RoiDayDistribution {
//		daysPercentageDistribution[days] = float64(count) / totalCount
//	}
//	result := make([]schema.ValueFrequencyPercentage, len(daysPercentageDistribution))
//	idx := 0
//	for value, frequencyPercentage := range daysPercentageDistribution {
//		result[idx] = schema.ValueFrequencyPercentage{
//			Value:               value,
//			FrequencyPercentage: frequencyPercentage,
//		}
//		idx += 1
//	}
//	sort.Slice(result, func(i, j int) bool {
//		return result[i].Value < result[j].Value
//	})
//	return result
//}

func getNewUsers(fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) map[string]int64 {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.UserBucketName),
			Key:    aws.String("per-user-join-time.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	newUsers := map[string]int64{}
	for address, userMetaInfo := range m {
		if schema.StarSharksContractAddresses[address] {
			continue
		}
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		userJoinTimestampObj := time.Unix(int64(timestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		if _, ok := schema.StarSharksGameWalletAddresses[address]; ok {
			continue
		}
		newUsers[address] = int64(timestamp)
	}
	return newUsers
}

func getAllTimeNewUsers(svc s3.S3) map[string]int64 {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.UserBucketName),
			Key:    aws.String("per-user-join-time.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	newUsers := map[string]int64{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		if _, ok := schema.StarSharksGameWalletAddresses[address]; ok {
			continue
		}
		newUsers[address] = int64(timestamp)
	}
	return newUsers
}

func extractNewUsersForTimeRange(allTimeNewUsers map[string]int64, fromTimeObj time.Time, toTimeObj time.Time) map[string]int64 {
	newUsers := map[string]int64{}
	for address, joinTimestamp := range allTimeNewUsers {
		userJoinTimestampObj := time.Unix(int64(joinTimestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		newUsers[address] = int64(joinTimestamp)
	}
	return newUsers
}

func getPriceHistory(tokenName string, fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) schema.PriceHistory {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.PriceBucketName),
			Key:    aws.String("sea-token-price-history.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}

	priceHistory := schema.PriceHistory{}
	err = json.Unmarshal(body, &priceHistory)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	return priceHistory
}

func getMysteriousBoxTransfers(fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) []schema.Transfer {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.PriceBucketName),
			Key:    aws.String("starsharks-mysterious-box-transfers.csv"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil || body == nil {
		exitErrorf("Unable to get body, %v", err)
	}
	bodyString := string(body)
	lines := strings.Split(bodyString, "\n")
	transfers := make([]schema.Transfer, 0)
	//log.Printf("enterred converCsvStringToTransferStructs, content len: %d", len(lines))
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 8 {
			continue
		}

		layout := "2006-01-02T15:04:05.000Z"
		timeObj, _ := time.Parse(layout, fields[2])
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		transfers = append(transfers, schema.Transfer{
			FromAddress:     fields[0],
			Value:           float64(40 * schema.SeaTokenUnit),
			TransactionHash: fields[1],
			Timestamp:       int(timeObj.Unix()),
		})
	}

	return transfers
}

func getPerPayerTransfers(transfers []schema.Transfer) map[string][]schema.Transfer {
	perUserTransfers := map[string][]schema.Transfer{}
	for _, transfer := range transfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]schema.Transfer, 0)
		}
	}
	return perUserTransfers
}

func GenerateResponse(respStruct interface{}) (events.APIGatewayProxyResponse, error) {
	response, err := json.Marshal(respStruct)
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type":                     "application/json",
			"Access-Control-Allow-Origin":      "*",
			"Access-Control-Allow-Credentials": "true",
			"Access-Control-Allow-Headers":     "Content-Type",
			"Access-Control-Allow-Methods":     "OPTIONS,POST,GET",
		},
		Body: string(response),
	}, err
}

func GetPayerType(transfers []schema.Transfer) schema.PayerType {
	if len(transfers) == 0 {
		return schema.Rentee
	}
	payerType := schema.Rentee
	if transfers[0].ContractAddress == schema.StarSharksRentContractAddresses {
		payerType = schema.Rentee
	} else if transfers[0].ContractAddress == schema.StarSharksPurchaseContractAddresses || transfers[0].ContractAddress == schema.StarSharksAuctionContractAddresses {
		payerType = schema.Purchaser
	}
	for _, transfer := range transfers {
		if payerType == schema.Hybrider {
			continue
		}
		if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
			if payerType == schema.Purchaser {
				payerType = schema.Hybrider
			}
		} else if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {

			if payerType == schema.Rentee {
				payerType = schema.Hybrider
			}
		}
	}
	return payerType
}

func GetPayerTypes(totalTransfers []schema.Transfer) map[string]schema.PayerType {
	userTypes := map[string]schema.PayerType{}

	for _, transfer := range totalTransfers {
		payerAddress := transfer.FromAddress
		if userTypes[payerAddress] == schema.Hybrider {
			continue
		}
		if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
			if userTypes[payerAddress] == schema.Purchaser {
				userTypes[payerAddress] = schema.Hybrider
			} else if userTypes[payerAddress] != schema.Rentee {
				userTypes[payerAddress] = schema.Rentee
			}
		}
		if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {
			if userTypes[payerAddress] == schema.Rentee {
				userTypes[payerAddress] = schema.Hybrider
			} else if userTypes[payerAddress] != schema.Purchaser {
				userTypes[payerAddress] = schema.Purchaser
			}
		}
	}
	return userTypes
}
