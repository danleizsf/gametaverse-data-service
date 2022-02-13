package main

import (
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserType(userAddress string) UserType {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	bucketName := "gametaverse-bucket"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
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
		totalTransfers = append(totalTransfers, transfers...)
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
	}
	payerTransfers := map[string][]Transfer{}

	for _, transfer := range totalTransfers {
		if transfer.FromAddress == userAddress || transfer.ToAddress == userAddress {
			payerTransfers[userAddress] = append(payerTransfers[userAddress], transfer)
		}
	}
	//perUserTransfers := getActiveUsersFromTransfers(transfers)
	payerType := GetPerPayerType(payerTransfers)[userAddress]
	transfers := payerTransfers[userAddress]
	if payerType == Rentee {
		return UserType{
			UserAddress: userAddress,
			Type:        "renter",
			Transfers:   transfers,
		}
	} else {
		return UserType{
			UserAddress: userAddress,
			Type:        "purchaser",
			Transfers:   transfers,
		}
	}
}
