package main

import (
	"context"
	"encoding/json"
	"gametaverse-data-service/grafana"
	"gametaverse-data-service/schema"
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
	input := schema.Input{}
	json.Unmarshal([]byte(request.Body), &input)
	log.Printf("path: %s, body: %s, httpmethod: %s", request.Path, request.Body, request.HTTPMethod)
	log.Printf("request: %v", request)
	log.Printf("Input: %v", input)
	if request.Path == "/grafana/search" {
		response := grafana.Search()
		return GenerateResponse(response)
	} else if request.Path == "/grafana/query" {
		grafanaQueryRequest := schema.GrafanaQueryRequest{}
		json.Unmarshal([]byte(request.Body), &grafanaQueryRequest)
		log.Printf("grafana/query body: %s", request.Body)
		log.Printf("grafana/query request: %v", grafanaQueryRequest)
		if grafanaQueryRequest.Targets[0].Target == "daus" {
			layout := "2006-01-02T15:04:05.000Z"
			fromTimeObj, _ := time.Parse(layout, grafanaQueryRequest.Range.From)
			toTimeObj, _ := time.Parse(layout, grafanaQueryRequest.Range.To)
			fromTimeDateObj := time.Unix((fromTimeObj.Unix()/int64(schema.DayInSec))*int64(schema.DayInSec), 0)
			toTimeDateObj := time.Unix((toTimeObj.Unix()/int64(schema.DayInSec))*int64(schema.DayInSec), 0)
			log.Printf("grafana/query request from %v, to %v", fromTimeDateObj, toTimeDateObj)
			daus := GetGameDaus(fromTimeDateObj, toTimeDateObj)
			response := grafana.ConverDausToMetrics(daus)
			return GenerateResponse(response)
		}
		return GenerateResponse("")
	} else if input.Method == "getDaus" {
		response := GetGameDaus(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if input.Method == "getDailyTransactionVolumes" {
		response := GetGameDailyTransactionVolumes(generateTimeObjs(input))
		return GenerateResponse(response)
		//} else if input.Method == "getUserData" {
		//	response := getUserData(input.Params[0].Address)
		//	return GenerateResponse(response)
	} else if input.Method == "getUserRetentionRate" {
		response := GetUserRetentionRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if input.Method == "getUserRepurchaseRate" {
		response := GetUserRepurchaseRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if input.Method == "getUserSpendingDistribution" {
		response := GetUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if input.Method == "getUserProfitDistribution" {
		userAddressMap := map[string]bool{}
		for _, param := range input.Params {
			userAddressMap[param.Address] = true
		}
		response := GetUserProfitDistribution(userAddressMap)
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if input.Method == "getUserRoi" {
		response := GetUserRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if input.Method == "getUserActiveDates" {
		response := GetUserActiveDates(starSharksStartingDate, time.Now(), input.Params[0].Limit)
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if input.Method == "getNewUserProfitableRate" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Now(), false)
		return GenerateResponse(response)
	} else if input.Method == "getNewUserProfitableRateDebug" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0), true)
		return GenerateResponse(response)
	} else if input.Method == "getUserType" {
		response := GetUserType()
		return GenerateResponse(response)
	} else if input.Method == "test" {
		tableNames, _ := dynamoDBClient.ListTables(nil)
		return GenerateResponse(tableNames.TableNames)
	}
	return GenerateResponse("")
}

func main() {
	lambda.Start(process)
}
