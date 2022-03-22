package main

import (
	daily "gametaverse-data-service/daily/functions"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	sess, _ := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				Region: aws.String("us-west-1"),
			},
			Profile: "bo-s3",
		},
	)

	s3client := s3.New(sess)
	c := lib.NewCache()
	daily.GetWhaleRois(s3client, c, schema.StarSharksStartingDate.Unix(), time.Now().Unix()-86400*2, schema.SortByProfit)
}
