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

type handler struct {
	dynamoDBClient *dynamodb.DynamoDB
}

func (h *handler) process(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
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
		layout := "2006-01-02T15:04:05.000Z"
		fromTimeObj, _ := time.Parse(layout, grafanaQueryRequest.Range.From)
		toTimeObj, _ := time.Parse(layout, grafanaQueryRequest.Range.To)
		fromTimeDateObj := time.Unix((fromTimeObj.Unix()/int64(schema.DayInSec))*int64(schema.DayInSec), 0)
		toTimeDateObj := time.Unix((toTimeObj.Unix()/int64(schema.DayInSec))*int64(schema.DayInSec), 0)
		if grafanaQueryRequest.Targets[0].Target == "daus" {
			log.Printf("grafana/query request from %v, to %v", fromTimeDateObj, toTimeDateObj)
			daus := GetGameDaus(fromTimeDateObj, toTimeDateObj)
			response := grafana.GetDauMetrics(daus)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "daily_transaction_volume" {
			dailyTransactionVolumes := GetGameDailyTransactionVolumes(fromTimeDateObj, toTimeDateObj)
			response := grafana.GetDailyTransactionVolumeMetrics(dailyTransactionVolumes)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profitable_rate" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, toTimeDateObj, false)
			response := grafana.GetNewUserProfitableRateMetrics(newUserProfitableRate.OverallProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_repurchase_rate" {
			userRepurchaseRate := GetUserRepurchaseRate(fromTimeDateObj, toTimeDateObj)
			response := grafana.GetUserRepurchaseRateMetrics(userRepurchaseRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_actual_active_dates_distribution" {
			userActiveDates := GetUserActiveDates(fromTimeDateObj, toTimeDateObj, 10000000)
			response := grafana.GetUserActualActiveDatesDistributionMetrics(userActiveDates)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_total_active_dates_distribution" {
			userActiveDates := GetUserActiveDates(fromTimeDateObj, toTimeDateObj, 10000000)
			response := grafana.GetUserTotalActiveDatesDistributionMetrics(userActiveDates)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_spending_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewUserSpendingUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_spending_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewRenteeSpendingUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_spending_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewPurchaserSpendingUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_spending_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewHybriderSpendingUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profit_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewUserProfitUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_profit_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewRenteeProfitUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_profit_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewPurchaserProfitUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_profit_usd_distribution" {
			newUserProfitableRate := GetNewUserProfitableRate(fromTimeDateObj, time.Now(), true)
			response := grafana.GetNewHybriderProfitUsdDistributionMetrics(newUserProfitableRate)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_type" {
			newUserTypes := GetUserType(fromTimeDateObj, time.Now())
			response := grafana.GetNewUserTypeMetrics(newUserTypes)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profitable_days" {
			userRois := GetUserRoi(fromTimeDateObj, time.Now())
			response := grafana.GetNewUserProfitableDaysDistributionMetrics(userRois)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_profitable_days" {
			userRois := GetUserRoi(fromTimeDateObj, time.Now())
			response := grafana.GetNewRenteeProfitableDaysDistributionMetrics(userRois)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_profitable_days" {
			userRois := GetUserRoi(fromTimeDateObj, time.Now())
			response := grafana.GetNewPurchaserProfitableDaysDistributionMetrics(userRois)
			return GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_profitable_days" {
			userRois := GetUserRoi(fromTimeDateObj, time.Now())
			response := grafana.GetNewHybriderProfitableDaysDistributionMetrics(userRois)
			return GenerateResponse(response)
		}
		return GenerateResponse("")
	} else if input.Method == "getDaus" {
		response := GetGameDaus(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return GenerateResponse(response)
	} else if input.Method == "getDailyTransactionVolumes" {
		response := GetGameDailyTransactionVolumes(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
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
		response := GetUserActiveDates(schema.StarSharksStartingDate, time.Now(), input.Params[0].Limit)
		return GenerateResponse(response)
		//return generateJsonResponse(response)
	} else if input.Method == "getNewUserProfitableRate" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Now(), false)
		return GenerateResponse(response)
	} else if input.Method == "getNewUserProfitableRateDebug" {
		response := GetNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0), true)
		return GenerateResponse(response)
	} else if input.Method == "getUserType" {
		fromTimeObj := schema.StarSharksStartingDate
		toTimeObj := time.Now()
		response := GetUserType(fromTimeObj, toTimeObj)
		return GenerateResponse(response)
	} else if input.Method == "test" {
		if h.dynamoDBClient == nil {
			return GenerateResponse("dynamoDBClient is nil")
		}
		tableNames, err := h.dynamoDBClient.ListTables(nil)
		log.Printf("test handler called, tableNames %v", tableNames)
		if err != nil {
			//log.Printf("body: %s", fmt.Sprintf("%s", body))
			exitErrorf("Unable to list Tables, %v", err)
		}

		return GenerateResponse(tableNames.TableNames)
	}
	return GenerateResponse("")
}

func main() {
	sess, _ := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config: aws.Config{
				Region: aws.String("us-west-1"),
			},
		},
	)
	h := handler{
		dynamoDBClient: dynamodb.New(sess),
	}

	lambda.Start(h.process)
}
