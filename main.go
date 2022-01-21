// main.go
package main

import (
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/service/s3"
)

func hello() (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)
	downloader := s3manager.NewDownloader(sess)

	result, err := svc.ListBuckets(nil)

	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	log.Printf("Buckets:")

	for _, bucket := range result.Buckets {
		log.Printf("* %s created on %s\n", aws.StringValue(bucket.Name), aws.TimeValue(bucket.CreationDate))
		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(*bucket.Name)})
		if err != nil {
			exitErrorf("Unable to list object, %v", err)
		}

		for _, item := range resp.Contents {
			log.Printf("file name: %s\n", *item.Key)
			file, err := os.Create(string("bo"))
			if err != nil {
				exitErrorf("Unable to create tmp file, %v", err)
			}
			numBytes, err := downloader.Download(file,
				&s3.GetObjectInput{
					Bucket: aws.String(*bucket.Name),
					Key:    aws.String(*item.Key),
				})
			if err != nil {
				exitErrorf("Unable to download file, %v", err)
			}
			log.Printf("Downloaded: %s, %s\n", *item.Key, string(numBytes))
		}
		log.Println()
	}
	return "address: {earning: $12}", nil
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func main() {
	// Make the handler available for Remove Procedure Call by AWS Lambda
	lambda.Start(hello)
}
