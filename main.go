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

func process(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	input := Input{}
	json.Unmarshal([]byte(request.Body), &input)
	log.Printf("path: %s, body: %s, httpmethod: %s", request.Path, request.Body, request.HTTPMethod)
	log.Printf("request: %v", request)
	if request.Path == "/grafana/search" {
		response := grafana.Search()
		return GenerateResponse(response)
	}
	if request.HTTPMethod == "getDaus" {
		response := GetGameDaus(generateTimeObjs(input))
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getDailyTransactionVolumes" {
		response := GetGameDailyTransactionVolumes(generateTimeObjs(input))
		return GenerateResponse(response)
		//} else if input.Method == "getUserData" {
		//	response := getUserData(input.Params[0].Address)
		//	return GenerateResponse(response)
	} else if request.HTTPMethod == "getUserRetentionRate" {
		response := GetUserRetentionRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getUserRepurchaseRate" {
		response := GetUserRepurchaseRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getUserSpendingDistribution" {
		response := GetUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getUserProfitDistribution" {
		userAddressMap := map[string]bool{}
		for _, param := range input.Params {
			userAddressMap[param.Address] = true
		}
		response := GetUserProfitDistribution(userAddressMap)
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if request.HTTPMethod == "getUserRoi" {
		response := GetUserRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if request.HTTPMethod == "getUserActiveDates" {
		response := GetUserActiveDates(starSharksStartingDate, time.Now(), input.Params[0].Limit)
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if request.HTTPMethod == "getNewUserProfitableRate" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Now(), false)
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getNewUserProfitableRateDebug" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0), true)
		return GenerateResponse(response)
	} else if request.HTTPMethod == "getUserType" {
		response := GetUserType()
		return GenerateResponse(response)
	} else if request.HTTPMethod == "test" {
		tableNames, _ := dynamoDBClient.ListTables(nil)
		return GenerateResponse(tableNames.TableNames)
	}
	return events.APIGatewayProxyResponse{}, nil
}

func main() {
	lambda.Start(process)
}
