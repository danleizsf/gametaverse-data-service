package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
}

type Dau struct {
	Date        string
	ActiveUsers int
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
		if len(fields) < 7 {
			continue
		}
		count += 1
		blockNumber, _ := strconv.Atoi(fields[6])
		value, _ := strconv.ParseFloat(fields[3], 64)
		logIndex, _ := strconv.Atoi(fields[5])
		if count < 8 {
			log.Printf("lineString: %s, fields: %v", lineString, fields)
			log.Printf("value: %v", value)
		}
		transfers = append(transfers, Transfer{
			TokenAddress:    fields[0],
			FromAddress:     fields[1],
			ToAddress:       fields[2],
			Value:           value,
			TransactionHash: fields[4],
			LogIndex:        logIndex,
			BlockNumber:     blockNumber,
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

func getTransactionVolumeFromTransfers(transfers []Transfer, timestamp int64) float64 {
	volume := float64(0)
	count := 0
	for _, transfer := range transfers {
		if count < 8 {
			log.Printf("transfer: %v, value: %v", transfer, transfer.Value/1000000000000000000)
		}
		count += 1
		volume += transfer.Value / 1000000000000000000
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

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")
	daus := make(map[int64]int)

	for _, bucket := range result.Buckets {
		log.Printf("* %s created on %s\n", aws.StringValue(bucket.Name), aws.TimeValue(bucket.CreationDate))

		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(*bucket.Name)})
		if err != nil {
			exitErrorf("Unable to list object, %v", err)
		}

		for _, item := range resp.Contents {
			log.Printf("file name: %s\n", *item.Key)
			timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
			time := time.Unix(timestamp, 0)
			eligibleToProcess := false
			for _, targetTime := range targetTimes {
				log.Printf("targetTime: %v, time: %v", targetTime, time)
				if targetTime.Year() == time.Year() || targetTime.Month() == time.Month() || targetTime.Day() == time.Day() {
					eligibleToProcess = true
					break
				}
			}
			if !eligibleToProcess {
				continue
			}
			log.Printf("filtered time: %v", time)

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
			//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
			daus[timestamp] = getActiveUserNumFromTransfers(transfers, int64(dateTimestamp))
		}
	}
	return daus
}

func getGameDailyTransactionVolumes() map[string]float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")
	dailyTransactionVolume := make(map[string]float64)

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
			dailyTransactionVolume[dateFormattedString] = getTransactionVolumeFromTransfers(transfers, int64(dateTimestamp))
		}
	}
	return dailyTransactionVolume
}

func getUserData(address string) (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")
	dailyTransactionVolume := make(map[string]float64)

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
			dailyTransactionVolume[dateFormattedString] = getUserTransactionVolume(address, transfers)
		}
	}
	return fmt.Sprintf("{starsharks: {dailyTransactionVolume: %v SEA Token}}", dailyTransactionVolume), nil
}

func process(ctx context.Context, input Input) (string, error) {
	log.Printf("intput: %v", input)
	if input.Method == "getDaus" {
		log.Printf("Input: %v", input)
		times := make([]time.Time, 0)
		for _, param := range input.Params {
			if param.Timestamp != 0 {
				times = append(times, time.Unix(param.Timestamp, 0))
			}
		}
		return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDau(times)), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDailyTransactionVolumes()), nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Params[0].Address)
	} else if input.Method == "getUserRetentionRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.25}", nil
	} else if input.Method == "getUserRepurchaseRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.75}", nil
	} else if input.Method == "getUserSpendingDistribution" {
		return "{\"jsonrpc\":\"2.0\",\"result\":[[10.0, 12.0, 13.0, 14.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 36.0, 37.0, 38.0, 39.0, 40.0, 42.0, 44.0, 45.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 72.0, 73.0, 74.0, 75.0, 76.0, 77.0, 78.0, 79.0, 80.0, 81.0, 82.0, 83.0, 84.0, 85.0, 87.0, 88.0, 89.0, 93.0, 94.0, 96.0, 97.0, 99.0, 100.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 111.0, 112.0, 114.0, 115.0, 116.0, 117.0, 121.0, 126.0, 127.0, 128.0, 130.0, 131.0, 132.0, 135.0, 136.0, 138.0, 139.0, 141.0, 144.0, 146.0, 147.0, 148.0, 149.0, 150.0, 151.0, 152.0, 153.0, 154.0, 155.0, 156.0, 157.0, 158.0, 159.0, 160.0, 161.0, 162.0, 163.0, 164.0, 166.0, 167.0, 170.0, 171.0, 173.0, 174.0, 175.0, 177.0, 179.0, 185.0, 197.0, 204.0, 209.0, 210.0, 211.0, 215.0, 222.0, 224.0, 228.0, 230.0, 231.0, 233.0, 236.0, 238.0, 258.0, 276.0, 300.0, 313.0, 324.0, 326.0, 327.0, 349.0, 354.0, 447.0, 450.0, 464.0, 500.0, 512.0, 527.0, 528.0, 532.0, 600.0, 800.0, 813.0, 900.0, 1000.0, 1050.0, 1150.0, 1200.0, 1327.0, 1350.0, 1500.0, 1600.0, 1800.0, 1950.0, 2000.0, 2150.0, 2500.0, 3000.0, 3150.0, 3450.0, 3500.0, 4500.0, 4650.0, 5000.0, 5300.0, 5900.0, 6500.0, 6900.0, 7500.0, 16550.0, 18500.0, 25699.99999999999], [0.0005518763796909492, 0.018211920529801324, 0.03863134657836645, 0.006070640176600441, 0.0005518763796909492, 0.002207505518763797, 0.0016556291390728477, 0.059602649006622516, 0.04470198675496689, 0.12306843267108168, 0.012693156732891833, 0.012693156732891833, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0027593818984547464, 0.0005518763796909492, 0.0011037527593818985, 0.009381898454746136, 0.0005518763796909492, 0.005518763796909493, 0.00772626931567329, 0.0005518763796909492, 0.0011037527593818985, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.010485651214128035, 0.0027593818984547464, 0.008278145695364239, 0.02041942604856512, 0.02924944812362031, 0.032560706401766004, 0.01545253863134658, 0.004966887417218543, 0.0027593818984547464, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.0011037527593818985, 0.002207505518763797, 0.011589403973509934, 0.0027593818984547464, 0.0027593818984547464, 0.0016556291390728477, 0.006070640176600441, 0.0005518763796909492, 0.004966887417218543, 0.002207505518763797, 0.0033112582781456954, 0.025386313465783666, 0.03532008830022075, 0.016004415011037526, 0.02704194260485651, 0.01379690949227373, 0.00717439293598234, 0.005518763796909493, 0.00717439293598234, 0.0027593818984547464, 0.0011037527593818985, 0.0011037527593818985, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.03863134657836645, 0.0016556291390728477, 0.0005518763796909492, 0.0016556291390728477, 0.009381898454746136, 0.0016556291390728477, 0.0033112582781456954, 0.010485651214128035, 0.0016556291390728477, 0.0005518763796909492, 0.0016556291390728477, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.0011037527593818985, 0.0011037527593818985, 0.0011037527593818985, 0.0005518763796909492, 0.0011037527593818985, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0016556291390728477, 0.0005518763796909492, 0.0005518763796909492, 0.0016556291390728477, 0.005518763796909493, 0.0033112582781456954, 0.002207505518763797, 0.01545253863134658, 0.008278145695364239, 0.017108167770419427, 0.004966887417218543, 0.006622516556291391, 0.004966887417218543, 0.009381898454746136, 0.013245033112582781, 0.009933774834437087, 0.006070640176600441, 0.004966887417218543, 0.004966887417218543, 0.0016556291390728477, 0.0027593818984547464, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0016556291390728477, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.0011037527593818985, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.009381898454746136, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.019867549668874173, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.002207505518763797, 0.0011037527593818985, 0.0005518763796909492, 0.002207505518763797, 0.012141280353200883, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.024282560706401765, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.0027593818984547464, 0.0011037527593818985, 0.0011037527593818985, 0.002207505518763797, 0.0011037527593818985, 0.0016556291390728477, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0016556291390728477, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0011037527593818985, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492, 0.0005518763796909492]]}", nil
	}
	return "", nil
}
func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(process)
}
