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
	TransactionType      string
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
}

type Dau struct {
	Date        string
	ActiveUsers int
}

type UserMetaInfo struct {
	Timestamp       int64  `json:"timestamp"`
	TransactionHash string `json:"transaction_hash"`
}

func converCsvStringToTransactionStructs(csvString string) []Transaction {
	lines := strings.Split(csvString, "\n")
	transactions := make([]Transaction, 0)
	count := 0
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 15 {
			continue
		}
		count += 1
		blockNumber, _ := strconv.Atoi(fields[3])
		transactionIndex, _ := strconv.Atoi(fields[4])
		value, _ := strconv.Atoi(fields[7])
		gas, _ := strconv.Atoi(fields[8])
		gasPrice, _ := strconv.Atoi(fields[9])
		blockTimestamp, _ := strconv.Atoi(fields[11])
		maxFeePerGas, _ := strconv.Atoi(fields[12])
		maxPriorityFeePerGas, _ := strconv.Atoi(fields[13])
		transactions = append(transactions, Transaction{
			TransactionHash:      fields[0],
			Nonce:                fields[1],
			BlockHash:            fields[2],
			BlockNumber:          blockNumber,
			TransactionIndex:     transactionIndex,
			FromAddress:          fields[5],
			ToAddress:            fields[6],
			Value:                value,
			Gas:                  gas,
			GasPrice:             gasPrice,
			Input:                fields[10],
			BlockTimestamp:       int64(blockTimestamp),
			MaxFeePerGas:         maxFeePerGas,
			MaxPriorityFeePerGas: maxPriorityFeePerGas,
			TransactionType:      fields[14],
		})
	}
	return transactions
}

func converCsvStringToTransferStructs(csvString string) []Transfer {
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
		if count < 8 {
			//log.Printf("lineString: %s, fields: %v", lineString, fields)
			//log.Printf("value: %v", value)
		}
		transfers = append(transfers, Transfer{
			TokenAddress:    fields[0],
			FromAddress:     fields[1],
			ToAddress:       fields[2],
			Value:           value,
			TransactionHash: fields[4],
			LogIndex:        logIndex,
			BlockNumber:     blockNumber,
			Timestamp:       timestamp,
		})
	}
	return transfers
}

func getDauFromTransactions(transactions []Transaction, timestamp int64) int {
	date := time.Unix(timestamp, 0).UTC()
	log.Printf("timestamp: %d, date: %s", timestamp, date)
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transaction := range transactions {
		transactionDate := time.Unix(transaction.BlockTimestamp, 0).UTC()
		if count < 8 {
			log.Printf("transaction: %v, transactionDate: %s, date: %s", transaction, transactionDate, date)
		}
		count += 1
		if transactionDate.Year() == date.Year() && transactionDate.Month() == date.Month() && transactionDate.Day() == date.Day() {
			uniqueAddresses[transaction.FromAddress] = true
			uniqueAddresses[transaction.ToAddress] = true
		}
	}
	return len(uniqueAddresses)
}

func getActiveUserNumFromTransfers(transfers []Transfer, timestamp int64) int {
	date := time.Unix(timestamp, 0).UTC()
	log.Printf("timestamp: %d, date: %s", timestamp, date)
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transfer := range transfers {
		if count < 8 {
			log.Printf("transfer: %v", transfer)
		}
		count += 1
		uniqueAddresses[transfer.FromAddress] = true
		uniqueAddresses[transfer.ToAddress] = true
	}
	return len(uniqueAddresses)
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

func getGameDau(targetTimes []time.Time) map[int64]int {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	daus := make(map[int64]int)

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
		bodyString := fmt.Sprintf("%s", body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := converCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
		daus[timestamp] = getActiveUserNumFromTransfers(transfers, int64(dateTimestamp))
	}
	return daus
}

func getGameDailyTransactionVolumes(targetTimeObjs []time.Time) map[int64]int64 {
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
		bodyString := fmt.Sprintf("%s", body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := converCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		dailyTransactionVolume[int64(dateTimestamp)] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
	}
	return dailyTransactionVolume
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
		bodyString := fmt.Sprintf("%s", body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := converCsvStringToTransferStructs(bodyString)
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

func getUserSpendingDistribution(fromTimeObj time.Time, toTimeObj time.Time) map[int64]float64 {
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
		bodyString := fmt.Sprintf("%s", body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := converCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserSpending := getPerUserSpending(totalTransfers)

	return generateSpendingDistribution(perUserSpending)
}

func getPerUserSpending(transfers []Transfer) map[string]int64 {
	perUserSpending := make(map[string]int64)
	for _, transfer := range transfers {
		if transfer.FromAddress == "0x0000000000000000000000000000000000000000" {
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

func generateSpendingDistribution(perUserSpending map[string]int64) map[int64]float64 {
	spendingValueDistribution := make(map[int64]int64)
	totalSpending := float64(0)
	for _, spending := range perUserSpending {
		if spending < 1 {
			continue
		}
		if _, ok := spendingValueDistribution[spending]; ok {
			spendingValueDistribution[spending] += 1
		} else {
			spendingValueDistribution[spending] = 1
		}
		totalSpending += float64(spending)
	}
	spendingPercentageDistribution := make(map[int64]float64)
	for spending, value := range spendingValueDistribution {
		spendingPercentageDistribution[spending] = float64(value) / totalSpending
	}
	return spendingPercentageDistribution
}
func process(ctx context.Context, input Input) (interface{}, error) {
	log.Printf("intput: %v", input)
	if input.Method == "getDaus" {
		log.Printf("Input: %v", input)

		//return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDau(generateTimeObjs(input))), nil
		return getGameDau(generateTimeObjs(input)), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		//return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDailyTransactionVolumes(generateTimeObjs(input))), nil
		response := getGameDailyTransactionVolumes(generateTimeObjs(input))
		log.Printf("getGameDailyTransactionVolumes returns: %v", response)
		return response, nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Params[0].Address)
	} else if input.Method == "getUserRetentionRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.25}", nil
	} else if input.Method == "getUserRepurchaseRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.75}", nil
	} else if input.Method == "getUserSpendingDistribution" {
		response := getUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getRoi" {
		response := getRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	}
	return "", nil
}

func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(process)
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

func generateJsonResponse(response interface{}) (string, error) {

	jsonString, _ := json.Marshal(response)
	return string(jsonString), nil
}

func getRoi(fromTimeObjs time.Time, toTimeObj time.Time) map[int64]float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	eligibleTransfers := make([]Transfer, 0)

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

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	targetUsers := map[string]bool{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		userJoinTimestampObj := time.Unix(int64(timestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObjs) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		targetUsers[address] = true
	}
	//log.Printf("targetUsers", targetUsers)

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
		bodyString := fmt.Sprintf("%s", body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := converCsvStringToTransferStructs(bodyString)
		eligibleTransfers = append(eligibleTransfers, transfers...)
	}

	targetUserTransfers := map[string][]Transfer{}

	for _, transfer := range eligibleTransfers {
		if _, ok := targetUsers[transfer.FromAddress]; ok {
			if _, ok := targetUserTransfers[transfer.FromAddress]; ok {
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			} else {
				targetUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
			}
		}
		if _, ok := targetUsers[transfer.ToAddress]; ok {
			if _, ok := targetUserTransfers[transfer.ToAddress]; ok {
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			} else {
				targetUserTransfers[transfer.ToAddress] = make([]Transfer, 0)
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
		initialTransferTimeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		profitTransferTimeObj := time.Unix(int64(transfers[transferIdx].Timestamp), 0)
		eligibleTargetUserRoi[userAddress] = int64(math.Ceil(profitTransferTimeObj.Sub(initialTransferTimeObj).Hours() / 24))
	}

	//log.Printf("eligibleTargetUserTransfers is: %v", eligibleTargetUserTransfers)
	//log.Printf("eligibleTargetUserRoi is: %v", eligibleTargetUserRoi)
	return generateRoiDistribution(eligibleTargetUserRoi)
}

func generateRoiDistribution(perUserRoiInDays map[string]int64) map[int64]float64 {
	RoiDayDistribution := make(map[int64]int64)
	totalDays := float64(0)
	for _, days := range perUserRoiInDays {
		if days < 1 {
			continue
		}
		if _, ok := RoiDayDistribution[days]; ok {
			RoiDayDistribution[days] += 1
		} else {
			RoiDayDistribution[days] = 1
		}
		totalDays += float64(days)
	}
	daysPercentageDistribution := make(map[int64]float64)
	for days, value := range RoiDayDistribution {
		daysPercentageDistribution[days] = float64(value) / totalDays
	}
	return daysPercentageDistribution
}
