package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"
)

type Input struct {
	Method string  `json:"method"`
	Params []Param `json:"params"`
}

type Param struct {
	Address       string `json:"address"`
	Timestamp     int64  `json:"timestamp"`
	FromTimestamp int64  `json:"fromTimestamp"`
	ToTimestamp   int64  `json:"toTimestamp"`
}

var dailyTransferBucketName = "gametaverse-bucket"
var userBucketName = "gametaverse-user-bucket"
var seaTokenUnit = 1000000000000000000
var starSharksGameWalletAddresses = map[string]bool{
	"0x0000000000000000000000000000000000000000": true,
	"0x1f7acc330fe462a9468aa47ecdb543787577e1e7": true,
}
var starSharksRentingContractAddresses = "0xe9e092e46a75d192d9d7d3942f11f116fd2f7ca9"
var starSharksPurchasingContractAddresses = "0x1f7acc330fe462a9468aa47ecdb543787577e1e7"

var starSharksStartingDate = time.Unix(1639612800, 0) // 12-16-2021

var dayInSec = 86400

type Dau struct {
	DateTimestamp    int64      `json:"dateTimestamp"`
	TotalActiveUsers PayerCount `json:"totalActiveUsers"`
	NewActiveUsers   PayerCount `json:"newActiveUsers"`
}

type PayerCount struct {
	RenterCount    int64 `json:"renterCount"`
	PurchaserCount int64 `json:"purchaserCount"`
}
type DailyTransactionVolume struct {
	DateTimestamp     int64 `json:"dateTimestamp"`
	TransactionVolume int64 `json:"transactionVolume"`
}

type ValueFrequencyPercentage struct {
	Value               int64   `json:"value"`
	FrequencyPercentage float64 `json:"frequencyPercentage"`
}

type UserActivity struct {
	UserAddress      string `json:"userAddress"`
	TotalDatesCount  int64  `json:"totalDatesCount"`
	ActiveDatesCount int64  `json:"activeDatesCount"`
}

type Transaction struct {
	TransactionHash      string
	Nonce                string
	BlockHash            string
	BlockNumber          int
	TransactionIndex     int
	FromAddress          string
	ToAddress            string
	Value                int
	Gas                  int
	GasPrice             int
	Input                string
	BlockTimestamp       int64
	MaxFeePerGas         int
	MaxPriorityFeePerGas int
}

type Transfer struct {
	TokenAddress    string
	FromAddress     string
	ToAddress       string
	Value           float64
	TransactionHash string
	LogIndex        int
	BlockNumber     int
	Timestamp       int
	ContractAddress string
}

type UserMetaInfo struct {
	Timestamp       int64  `json:"timestamp"`
	TransactionHash string `json:"transaction_hash"`
}

type payerType int64

const (
	Renter    payerType = 0
	Purchaser payerType = 1
)

func process(ctx context.Context, input Input) (interface{}, error) {
	log.Printf("intput: %v", input)
	if input.Method == "getDaus" {
		return getGameDaus(generateTimeObjs(input)), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		response := getGameDailyTransactionVolumes(generateTimeObjs(input))
		return response, nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Params[0].Address)
	} else if input.Method == "getUserRetentionRate" {
		response := getUserRetentionRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserRepurchaseRate" {
		response := getUserRepurchaseRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserSpendingDistribution" {
		response := getUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserProfitDistribution" {
		response := getUserProfitDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserRoi" {
		response := getUserRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserActiveDates" {
		response := getUserActiveDates(starSharksStartingDate, time.Now())
		return response, nil
		//return generateJsonResponse(response)
	}
	return "", nil
}

func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(process)
}

//func converCsvStringToTransactionStructs(csvString string) []Transaction {
//	lines := strings.Split(csvString, "\n")
//	transactions := make([]Transaction, 0)
//	count := 0
//	for lineNum, lineString := range lines {
//		if lineNum == 0 {
//			continue
//		}
//		fields := strings.Split(lineString, ",")
//		if len(fields) < 15 {
//			continue
//		}
//		count += 1
//		blockNumber, _ := strconv.Atoi(fields[3])
//		transactionIndex, _ := strconv.Atoi(fields[4])
//		value, _ := strconv.Atoi(fields[7])
//		gas, _ := strconv.Atoi(fields[8])
//		gasPrice, _ := strconv.Atoi(fields[9])
//		blockTimestamp, _ := strconv.Atoi(fields[11])
//		maxFeePerGas, _ := strconv.Atoi(fields[12])
//		maxPriorityFeePerGas, _ := strconv.Atoi(fields[13])
//		transactions = append(transactions, Transaction{
//			TransactionHash:      fields[0],
//			Nonce:                fields[1],
//			BlockHash:            fields[2],
//			BlockNumber:          blockNumber,
//			TransactionIndex:     transactionIndex,
//			FromAddress:          fields[5],
//			ToAddress:            fields[6],
//			Value:                value,
//			Gas:                  gas,
//			GasPrice:             gasPrice,
//			Input:                fields[10],
//			BlockTimestamp:       int64(blockTimestamp),
//			MaxFeePerGas:         maxFeePerGas,
//			MaxPriorityFeePerGas: maxPriorityFeePerGas,
//			TransactionType:      fields[14],
//		})
//	}
//	return transactions
//}

func convertCsvStringToTransferStructs(csvString string) []Transfer {
	lines := strings.Split(csvString, "\n")
	transfers := make([]Transfer, 0)
	count := 0
	log.Printf("enterred converCsvStringToTransferStructs, content len: %d", len(lines))
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
		transfers = append(transfers, Transfer{
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
	return transfers
}

//func getDauFromTransactions(transactions []Transaction, timestamp int64) int {
//	date := time.Unix(timestamp, 0).UTC()
//	log.Printf("timestamp: %d, date: %s", timestamp, date)
//	uniqueAddresses := make(map[string]bool)
//	count := 0
//	for _, transaction := range transactions {
//		transactionDate := time.Unix(transaction.BlockTimestamp, 0).UTC()
//		if count < 8 {
//			log.Printf("transaction: %v, transactionDate: %s, date: %s", transaction, transactionDate, date)
//		}
//		count += 1
//		if transactionDate.Year() == date.Year() && transactionDate.Month() == date.Month() && transactionDate.Day() == date.Day() {
//			uniqueAddresses[transaction.FromAddress] = true
//			uniqueAddresses[transaction.ToAddress] = true
//		}
//	}
//	return len(uniqueAddresses)
//}

func getActiveUsersFromTransfers(transfers []Transfer) map[string]bool {
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transfer := range transfers {
		count += 1
		uniqueAddresses[transfer.FromAddress] = true
		uniqueAddresses[transfer.ToAddress] = true
	}
	return uniqueAddresses
}

func getUserTransactionVolume(address string, transfers []Transfer) float64 {
	transactionVolume := float64(0)
	for _, transfer := range transfers {
		if transfer.FromAddress == address || transfer.ToAddress == address {
			transactionVolume += transfer.Value
			log.Printf("address: %s, transactionHash: %s, value: %v", address, transfer.TransactionHash, transfer.Value)
		}
	}
	return transactionVolume / 1000000000000000000
}

func getTransactionVolumeFromTransfers(transfers []Transfer, timestamp int64) int64 {
	volume := int64(0)
	count := 0
	for _, transfer := range transfers {
		if count < 8 {
			log.Printf("transfer: %v, value: %v", transfer, transfer.Value/1000000000000000000)
		}
		count += 1
		volume += int64(transfer.Value / 1000000000000000000)
	}
	return volume
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func getGameDaus(targetTimes []time.Time) []Dau {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	daus := make(map[int64]Dau)

	bucketName := "gametaverse-bucket"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if !isEligibleToProcess(timeObj, targetTimes) {
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
		perPayerTransfers := getPerPayerTransfers(transfers)
		//perUserTransfers := getActiveUsersFromTransfers(transfers)
		totalPerPayerType := getPerPayerType(perPayerTransfers)
		totalRenterCount, totalPurchaserCount := 0, 0
		for _, payerType := range totalPerPayerType {
			if payerType == Renter {
				totalRenterCount += 1
			} else if payerType == Purchaser {
				totalPurchaserCount += 1
			}
		}

		newUsers := getNewUsers(timeObj, time.Unix(timestamp+int64(dayInSec), 0), *svc)
		perNewPayerTransfers := map[string][]Transfer{}
		for payerAddress, transfers := range perPayerTransfers {
			if _, ok := newUsers[payerAddress]; ok {
				perNewPayerTransfers[payerAddress] = transfers
			}
		}
		perNewPayerType := getPerPayerType(perNewPayerTransfers)
		newRenterCount, newPurchaserCount := 0, 0
		for _, payerType := range perNewPayerType {
			if payerType == Renter {
				newRenterCount += 1
			} else if payerType == Purchaser {
				newPurchaserCount += 1
			}
		}
		daus[timestamp] = Dau{
			DateTimestamp: timestamp,
			TotalActiveUsers: PayerCount{
				RenterCount:    int64(totalRenterCount),
				PurchaserCount: int64(totalPurchaserCount),
			},
			NewActiveUsers: PayerCount{
				RenterCount:    int64(newRenterCount),
				PurchaserCount: int64(newPurchaserCount),
			},
		}
	}
	result := make([]Dau, len(daus))
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

func getGameDailyTransactionVolumes(targetTimeObjs []time.Time) []DailyTransactionVolume {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	dailyTransactionVolume := make(map[int64]int64)

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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		dailyTransactionVolume[int64(dateTimestamp)] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
	}

	result := make([]DailyTransactionVolume, len(dailyTransactionVolume))
	idx := 0
	for dateTimestamp, transactionVolume := range dailyTransactionVolume {
		result[idx] = DailyTransactionVolume{
			DateTimestamp:     dateTimestamp,
			TransactionVolume: transactionVolume,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DateTimestamp < result[j].DateTimestamp
	})
	return result
}

func getUserData(address string) (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	dailyTransactionVolume := make(map[string]float64)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		dateObj := time.Unix(int64(dateTimestamp), 0).UTC()
		dateFormattedString := fmt.Sprintf("%d-%d-%d", dateObj.Year(), dateObj.Month(), dateObj.Day())
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
		dailyTransactionVolume[dateFormattedString] = getUserTransactionVolume(address, transfers)
	}
	return fmt.Sprintf("{starsharks: {dailyTransactionVolume: %v SEA Token}}", dailyTransactionVolume), nil
}

func getUserSpendingDistribution(fromTimeObj time.Time, toTimeObj time.Time) []ValueFrequencyPercentage {
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserSpending := getPerUserSpending(totalTransfers)

	return generateValueDistribution(perUserSpending)
}

func getPerUserSpending(transfers []Transfer) map[string]int64 {
	perUserSpending := make(map[string]int64)
	for _, transfer := range transfers {
		if _, ok := starSharksGameWalletAddresses[transfer.FromAddress]; ok {
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

func generateValueDistribution(perUserValue map[string]int64) []ValueFrequencyPercentage {
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
	result := make([]ValueFrequencyPercentage, len(valuePercentageDistribution))
	idx := 0
	for value, percentage := range valuePercentageDistribution {
		result[idx] = ValueFrequencyPercentage{
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

func isEligibleToProcess(timeObj time.Time, targetTimeObjs []time.Time) bool {
	eligibleToProcess := false
	for _, targetTimeObj := range targetTimeObjs {
		log.Printf("targetTime: %v, time: %v", targetTimeObj, timeObj)
		if targetTimeObj.Year() == timeObj.Year() && targetTimeObj.Month() == timeObj.Month() && targetTimeObj.Day() == timeObj.Day() {
			eligibleToProcess = true
			break
		}
	}
	return eligibleToProcess
}

func generateTimeObjs(input Input) []time.Time {
	times := make([]time.Time, 0)
	for _, param := range input.Params {
		if param.Timestamp != 0 {
			times = append(times, time.Unix(param.Timestamp, 0))
		}
	}
	return times
}

func getUserRoi(fromTimeObjs time.Time, toTimeObj time.Time) []ValueFrequencyPercentage {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	eligibleTransfers := make([]Transfer, 0)
	targetUsers := getNewUsers(fromTimeObjs, toTimeObj, *svc)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObjs) {
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		eligibleTransfers = append(eligibleTransfers, transfers...)
	}

	targetUserTransfers := map[string][]Transfer{}

	for _, transfer := range eligibleTransfers {
		if _, ok := targetUsers[transfer.FromAddress]; ok {
			if _, ok := targetUserTransfers[transfer.FromAddress]; ok {
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			} else {
				targetUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			}
		}
		if _, ok := targetUsers[transfer.ToAddress]; ok {
			if _, ok := targetUserTransfers[transfer.ToAddress]; ok {
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			} else {
				targetUserTransfers[transfer.ToAddress] = make([]Transfer, 0)
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			}
		}
	}

	for userAddress, transfers := range targetUserTransfers {
		sort.Slice(targetUserTransfers[userAddress], func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
	}

	eligibleTargetUserTransfers := map[string][]Transfer{}
	for userAddress, transfers := range targetUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		timeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		if timeObj.Before(fromTimeObjs) || timeObj.After(toTimeObj) {
			continue
		}
		eligibleTargetUserTransfers[userAddress] = transfers
	}

	eligibleTargetUserRoi := map[string]int64{}
	for userAddress, transfers := range eligibleTargetUserTransfers {
		value := -1
		transferIdx := -1
		for _, transfer := range transfers {
			if transfer.FromAddress == userAddress {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("spend %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value -= int(transfer.Value / 1000000000000000000)
			} else {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("earn %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value += int(transfer.Value / 1000000000000000000)
			}
			transferIdx += 1
			if value > 0 {
				break
			}
		}

		if value < 0 {
			continue
		}

		initialTransferTimeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		profitTransferTimeObj := time.Unix(int64(transfers[transferIdx].Timestamp), 0)
		eligibleTargetUserRoi[userAddress] = int64(math.Ceil(profitTransferTimeObj.Sub(initialTransferTimeObj).Hours() / 24))
	}

	return generateRoiDistribution(eligibleTargetUserRoi)
}

func generateRoiDistribution(perUserRoiInDays map[string]int64) []ValueFrequencyPercentage {
	RoiDayDistribution := make(map[int64]int64)
	totalCount := float64(0)
	for _, days := range perUserRoiInDays {
		if days < 1 {
			continue
		}
		RoiDayDistribution[days] += 1
		totalCount += 1
	}
	daysPercentageDistribution := make(map[int64]float64)
	for days, count := range RoiDayDistribution {
		daysPercentageDistribution[days] = float64(count) / totalCount
	}
	result := make([]ValueFrequencyPercentage, len(daysPercentageDistribution))
	idx := 0
	for value, frequencyPercentage := range daysPercentageDistribution {
		result[idx] = ValueFrequencyPercentage{
			Value:               value,
			FrequencyPercentage: frequencyPercentage,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Value < result[j].Value
	})
	return result
}

func getUserRetentionRate(fromTimeObj time.Time, toTimeObj time.Time) float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)
	fromDateTimestamp := fromTimeObj.Unix()
	toDateTimestamp := toTimeObj.Unix()

	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(dailyTransferBucketName),
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
	fromDateTransfers := convertCsvStringToTransferStructs(bodyString)

	requestInput =
		&s3.GetObjectInput{
			Bucket: aws.String(dailyTransferBucketName),
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
	toDateTransfers := convertCsvStringToTransferStructs(bodyString)

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

func getNewUsers(fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) map[string]bool {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(userBucketName),
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

	newUsers := map[string]bool{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		userJoinTimestampObj := time.Unix(int64(timestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		newUsers[address] = true
	}
	return newUsers
}

func getUserRepurchaseRate(fromTimeObj time.Time, toTimeObj time.Time) float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserTransfers := map[string][]Transfer{}
	repurchaseUserCount := 0
	for _, transfer := range totalTransfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
	}
	for _, transfers := range perUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		sort.Slice(transfers, func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
		if transfers[len(transfers)-1].Timestamp-transfers[0].Timestamp > dayInSec {
			repurchaseUserCount += 1
		}
	}
	log.Printf("total user count: %d, repurhase user count: %d", len(perUserTransfers), repurchaseUserCount)
	return float64(repurchaseUserCount) / float64(len(perUserTransfers))
}

func getUserActiveDates(fromTimeObj time.Time, toTimeObj time.Time) []UserActivity {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserTransfers := map[string][]Transfer{}
	for _, transfer := range totalTransfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
	}
	perUserActivities := make([]UserActivity, 0) //len(perUserTransfers))
	idx := 0
	for userAddress, transfers := range perUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		sort.Slice(transfers, func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
		totalDatesCount := transfers[len(transfers)-1].Timestamp/dayInSec - transfers[0].Timestamp/dayInSec + 1
		activeDatesCount := 1
		currentDate := transfers[0].Timestamp / dayInSec
		for _, transfer := range transfers {
			if transfer.Timestamp/dayInSec != currentDate {
				activeDatesCount += 1
				currentDate = transfer.Timestamp / dayInSec
			}
		}

		//if userAddress == "0x27eafaf87860c290c185c1105cbedeb3b742c748" {
		//	log.Printf("for user %s, totalDatesCount %d, activeDatesCount %d", userAddress, totalDatesCount, activeDatesCount)
		//	for _, transfer := range transfers {
		//		log.Printf("transfer timestamp %d, date %d", transfer.Timestamp, transfer.Timestamp/dayInSec)
		//	}
		//	perUserActivities[idx] = UserActivity{UserAddress: userAddress, TotalDatesCount: int64(totalDatesCount), ActiveDatesCount: int64(activeDatesCount)}
		//}
		perUserActivities = append(perUserActivities, UserActivity{UserAddress: userAddress, TotalDatesCount: int64(totalDatesCount), ActiveDatesCount: int64(activeDatesCount)})
		idx += 1
	}
	return perUserActivities
}

func getUserProfitDistribution(fromTimeObj time.Time, toTimeObj time.Time) []ValueFrequencyPercentage {
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
		transfers := convertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserProfit := make(map[string]int64)
	for _, transfer := range totalTransfers {
		if _, ok := starSharksGameWalletAddresses[transfer.FromAddress]; ok {
			continue
		}
		if _, ok := starSharksGameWalletAddresses[transfer.ToAddress]; ok {
			continue
		}
		perUserProfit[transfer.FromAddress] -= int64(transfer.Value / float64(seaTokenUnit))
		perUserProfit[transfer.ToAddress] += int64(transfer.Value / float64(seaTokenUnit))
	}

	return generateValueDistribution(perUserProfit)
}

func getPerPayerTransfers(transfers []Transfer) map[string][]Transfer {
	perUserTransfers := map[string][]Transfer{}
	for _, transfer := range transfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
	}
	return perUserTransfers
}

func getPerPayerType(perPayerTransfers map[string][]Transfer) map[string]payerType {
	perPayerType := map[string]payerType{}
	for payerAddress, transfers := range perPayerTransfers {
		totalRentingValue := float64(0)
		totalInvestingValue := float64(0)
		for _, transfer := range transfers {
			if transfer.ContractAddress == starSharksPurchasingContractAddresses {
				totalInvestingValue += transfer.Value / float64(dayInSec)
			} else if transfer.ContractAddress == starSharksRentingContractAddresses {
				totalRentingValue += transfer.Value / float64(dayInSec)
			}
		}
		if totalInvestingValue > totalRentingValue {
			perPayerType[payerAddress] = Purchaser
		} else {
			perPayerType[payerAddress] = Renter
		}
	}
	return perPayerType
}
