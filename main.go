package main

import (
	"context"
	"encoding/json"
	"gametaverse-data-service/grafana"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var dynamoDBClient *dynamodb.DynamoDB

func init() {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	// Create DynamoDB client
	dynamoDBClient = dynamodb.New(sess)
}

func process(ctx context.Context, request events.APIGatewayProxyRequest) (interface{}, error) {
	input := Input{}
	json.Unmarshal([]byte(request.Body), &input)
	log.Printf("path: %s, body: %s", request.Path, request.Body)
	log.Printf("request: %v", request)
	if request.Path == "/grafana/search/grafana/search" {
		return grafana.Search(), nil
	}
	if input.Method == "getDaus" {
		return GetGameDaus(generateTimeObjs(input)), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		response := GetGameDailyTransactionVolumes(generateTimeObjs(input))
		return response, nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Params[0].Address)
	} else if input.Method == "getUserRetentionRate" {
		response := GetUserRetentionRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserRepurchaseRate" {
		response := GetUserRepurchaseRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserSpendingDistribution" {
		response := GetUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserProfitDistribution" {
		userAddressMap := map[string]bool{}
		for _, param := range input.Params {
			userAddressMap[param.Address] = true
		}
		response := GetUserProfitDistribution(userAddressMap)
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserRoi" {
		response := GetUserRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserActiveDates" {
		response := GetUserActiveDates(starSharksStartingDate, time.Now(), input.Params[0].Limit)
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getNewUserProfitableRate" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Now(), false)
		return response, nil
	} else if input.Method == "getNewUserProfitableRateDebug" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0), true)
		return response, nil
	} else if input.Method == "getUserType" {
		response := GetUserType()
		return response, nil
	} else if input.Method == "test" {
		tableNames, err := dynamoDBClient.ListTables(nil)
		return tableNames.TableNames, err
	}
	return "", nil
}

func main() {
	lambda.Start(process)
}
