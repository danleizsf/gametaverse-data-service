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
	Method string `json:"method"`
	Data   string `json:"data"`
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

func getGameDau() map[string]int {
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
		return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDau()), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		return fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"result\":%v}", getGameDailyTransactionVolumes()), nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Data)
	} else if input.Method == "getUserRetentionRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.25}", nil
	} else if input.Method == "getUserRepurchaseRate" {
		return "{\"jsonrpc\":\"2.0\",\"result\":0.75}", nil
	} else if input.Method == "getUserSpendingDistribution" {
		return "{\"jsonrpc\":\"2.0\",\"result\":{61.0: 1, 13.0: 70, 96.0: 70, 24.0: 108, 38.0: 10, 12.0: 33, 48.0: 19, 66.0: 5, 81.0: 13, 300.0: 17, 104.0: 6, 50.0: 15, 25.0: 81, 160.0: 9, 26.0: 223, 68.0: 11, 74.0: 46, 70.0: 9, 151.0: 15, 63.0: 4, 157.0: 24, 185.0: 1, 39.0: 14, 150.0: 28, 52.0: 53, 64.0: 21, 36.0: 17, 153.0: 9, 161.0: 9, 327.0: 1, 154.0: 12, 147.0: 10, 27.0: 23, 55.0: 9, 54.0: 28, 1000.0: 22, 25699.99999999999: 1, 78.0: 25, 14.0: 11, 230.0: 1, 152.0: 31, 215.0: 1, 77.0: 49, 75.0: 64, 28.0: 23, 22.0: 4, 900.0: 4, 102.0: 17, 80.0: 10, 49.0: 5, 73.0: 6, 1500.0: 44, 53.0: 59, 103.0: 3, 34.0: 2, 105.0: 19, 163.0: 5, 23.0: 3, 159.0: 11, 32.0: 5, 158.0: 18, 69.0: 1, 51.0: 37, 512.0: 1, 175.0: 1, 144.0: 1, 500.0: 36, 37.0: 1, 155.0: 9, 156.0: 17, 62.0: 2, 1600.0: 1, 173.0: 1, 166.0: 1, 117.0: 1, 44.0: 2, 177.0: 1, 148.0: 6, 4650.0: 1, 127.0: 2, 1350.0: 1, 3150.0: 2, 141.0: 1, 131.0: 1, 4500.0: 1, 2000.0: 5, 100.0: 3, 76.0: 29, 324.0: 1, 89.0: 1, 600.0: 4, 2500.0: 2, 447.0: 1, 67.0: 3, 354.0: 1, 79.0: 13, 204.0: 2, 527.0: 1, 1050.0: 1, 179.0: 1, 31.0: 1, 5000.0: 3, 7500.0: 1, 111.0: 1, 326.0: 2, 135.0: 2, 211.0: 2, 2150.0: 2, 209.0: 1, 349.0: 1, 233.0: 1, 464.0: 1, 6500.0: 1, 72.0: 4, 224.0: 1, 93.0: 1, 149.0: 4, 231.0: 1, 85.0: 2, 58.0: 2, 1950.0: 1, 3450.0: 3, 1800.0: 2, 47.0: 1, 139.0: 3, 40.0: 1, 236.0: 1, 108.0: 3, 42.0: 2, 222.0: 1, 87.0: 1, 82.0: 5, 83.0: 2, 84.0: 2, 65.0: 5, 450.0: 2, 1327.0: 1, 121.0: 1, 174.0: 1, 146.0: 3, 33.0: 1, 210.0: 2, 228.0: 1, 16550.0: 1, 197.0: 1, 813.0: 1, 1200.0: 1, 94.0: 2, 130.0: 2, 1150.0: 1, 45.0: 1, 3000.0: 4, 276.0: 1, 114.0: 1, 21.0: 1, 56.0: 5, 800.0: 2, 132.0: 2, 106.0: 3, 116.0: 1, 170.0: 3, 167.0: 1, 138.0: 1, 97.0: 3, 3500.0: 2, 313.0: 1, 171.0: 1, 30.0: 1, 258.0: 1, 238.0: 1, 5300.0: 1, 5900.0: 1, 162.0: 3, 164.0: 2, 6900.0: 2, 29.0: 1, 88.0: 1, 115.0: 1, 18500.0: 1, 112.0: 1, 532.0: 1, 136.0: 1, 107.0: 1, 528.0: 1, 128.0: 2, 126.0: 2, 10.0: 1, 57.0: 1, 99.0: 1}}", nil
	}
	return "", nil
}
func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(process)
}
