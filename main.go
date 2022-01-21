// main.go
package main

import (
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

type Dau struct {
	Date        string
	ActiveUsers int
}

func hello() (string, error) {
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
			transactions := converCsvStringToTransactionStructs(bodyString)
			dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
			daus[int64(dateTimestamp)] = getDau(transactions, int64(dateTimestamp))
		}
	}
	return fmt.Sprintf("daus: %v", daus), nil
}

func converCsvStringToTransactionStructs(csvString string) []Transaction {
	lines := strings.Split(csvString, "\n")
	transactions := make([]Transaction, len(lines)-1)
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 15 {
			continue
		}
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

func getDau(transactions []Transaction, timestamp int64) int {
	date := time.Unix(timestamp, 0).UTC()
	log.Printf("timestamp: %d, date: %s", timestamp, date)
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transaction := range transactions {
		transactionDate := time.Unix(transaction.BlockTimestamp, 0).UTC()
		if count < 8 {
			log.Printf("transactionData: %s, date: %s", transactionDate, date)
		}
		count += 1
		if transactionDate.Equal(date) {
			uniqueAddresses[transaction.FromAddress] = true
			uniqueAddresses[transaction.ToAddress] = true
		}
	}
	return len(uniqueAddresses)
}
func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(hello)
}
