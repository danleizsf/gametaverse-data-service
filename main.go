package main

import (
	"context"
	"encoding/json"
	daily "gametaverse-data-service/daily/functions"
	"gametaverse-data-service/grafana"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type handler struct {
	s3Client *s3.S3
	cache    *lib.Cache
}

func (h *handler) process(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	input := schema.Input{}
	json.Unmarshal([]byte(request.Body), &input)
	log.Printf("path: %s, body: %s, httpmethod: %s", request.Path, request.Body, request.HTTPMethod)
	log.Printf("request: %v", request)
	log.Printf("Input: %v", input)
	if request.Path == "/grafana/search" {
		response := grafana.Search()
		return lib.GenerateResponse(response)
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
		// Warm up cache
		if strings.HasSuffix(grafanaQueryRequest.Targets[0].Target, "2") {
			go lib.GetUserActionsRangeAsync(h.s3Client, h.cache, fromTimeObj.Unix(), toTimeObj.Unix())
		}
		if grafanaQueryRequest.Targets[0].Target == "daus2" {
			daus := daily.GetDaus(h.s3Client, h.cache, fromTimeDateObj, toTimeDateObj)
			response := grafana.GetDauMetrics(daus)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "daily_transaction_volume2" {
			dailyTransactionVolumes := daily.GetTransactionVolumes(h.s3Client, h.cache, fromTimeObj, toTimeObj)
			response := grafana.GetDailyTransactionVolumeMetrics(dailyTransactionVolumes)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_repurchase_rate2" {
			userRepurchaseRate := daily.GetUserRepurchaseRate(h.s3Client, h.cache, fromTimeObj.Unix(), toTimeObj.Unix())
			response := grafana.GetUserRepurchaseRateMetrics(userRepurchaseRate)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_actual_active_dates_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), toTimeObj.Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "user_actual_active_dates_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userActiveDates := daily.GetUserActiveDays(h.s3Client, h.cache, fromTimeObj.Unix(), toTimeObj.Unix(), 1000000)
			response := grafana.GetUserActualActiveDatesDistributionMetrics(userActiveDates)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "user_actual_active_dates_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "user_total_active_dates_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), toTimeObj.Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "user_total_active_dates_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userActiveDates := daily.GetUserActiveDays(h.s3Client, h.cache, fromTimeObj.Unix(), toTimeObj.Unix(), 1000000)
			response := grafana.GetUserTotalActiveDatesDistributionMetrics(userActiveDates)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "user_total_active_dates_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_type2" {
			newUserTypes := daily.GetUserType(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix())
			response := grafana.GetNewUserTypeMetrics(newUserTypes)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profitable_rate2" {
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), toTimeObj.Unix(), false, fromTimeObj, toTimeObj)
			response := grafana.GetNewUserProfitableRateMetrics(newUserProfitableRate.OverallProfitableRate)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_spending_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_user_spending_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewUserSpendingUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_user_spending_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_spending_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_rentee_spending_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewRenteeSpendingUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_rentee_spending_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_spending_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_rentee_spending_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewRenteeSpendingTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_rentee_spending_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_spending_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_purchaser_spending_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewPurchaserSpendingUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_purchaser_spending_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_spending_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_purchaser_spending_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewPurchaserSpendingTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_purchaser_spending_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_spending_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_hybrider_spending_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewHybriderSpendingUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_hybrider_spending_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_spending_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_hybrider_spending_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewHybriderSpendingTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_hybrider_spending_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profit_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_user_profit_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewUserProfitUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_user_profit_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_profit_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_rentee_profit_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewRenteeProfitUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_rentee_profit_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_profit_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_rentee_profit_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewRenteeProfitTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_rentee_profit_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_profit_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profit_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewPurchaserProfitUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profit_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_profit_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profit_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewPurchaserProfitTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profit_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_profit_usd_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profit_usd_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewHybriderProfitUsdDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profit_usd_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_profit_token_distribution2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profit_token_distribution2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			newUserProfitableRate := daily.GetNewUserProfitableRate(h.s3Client, h.cache, fromTimeObj.Unix(), time.Now().Unix(), true, fromTimeObj, toTimeObj)
			response := grafana.GetNewHybriderProfitTokenDistributionMetrics(newUserProfitableRate)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profit_token_distribution2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_user_profitable_days2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_user_profitable_days2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userRois := daily.GetNewUserRoi(h.s3Client, h.cache, fromTimeObj, time.Now())
			response := grafana.GetNewUserProfitableDaysDistributionMetrics(userRois)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_user_profitable_days2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_rentee_profitable_days2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_rentee_profitable_days2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userRois := daily.GetNewUserRoi(h.s3Client, h.cache, fromTimeObj, time.Now())
			response := grafana.GetNewRenteeProfitableDaysDistributionMetrics(userRois)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_rentee_profitable_days2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_purchaser_profitable_days2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profitable_days2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userRois := daily.GetNewUserRoi(h.s3Client, h.cache, fromTimeObj, time.Now())
			response := grafana.GetNewPurchaserProfitableDaysDistributionMetrics(userRois)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_purchaser_profitable_days2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "new_hybrider_profitable_days2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profitable_days2"); exist {
				json.Unmarshal(body, &resp)
				return lib.GenerateResponse(resp)
			}
			userRois := daily.GetNewUserRoi(h.s3Client, h.cache, fromTimeObj, time.Now())
			response := grafana.GetNewHybriderProfitableDaysDistributionMetrics(userRois)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "new_hybrider_profitable_days2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "whale_sort_by_gain2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_gain2"); exist {
				if err := json.Unmarshal(body, &resp); err != nil {
					return lib.GenerateResponse(resp)
				}
			}
			whaleRois := daily.GetWhaleRois(h.s3Client, h.cache, schema.StarSharksStartingDate.Unix(), time.Now().Unix(), schema.SortByGain)
			response := grafana.GetWhaleRoisMetrics(whaleRois, schema.SortByGain)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_gain2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "whale_sort_by_profit2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_profit2"); exist {
				if err := json.Unmarshal(body, &resp); err != nil {
					return lib.GenerateResponse(resp)
				}
			}
			whaleRois := daily.GetWhaleRois(h.s3Client, h.cache, schema.StarSharksStartingDate.Unix(), time.Now().Unix(), schema.SortByProfit)
			response := grafana.GetWhaleRoisMetrics(whaleRois, schema.SortByProfit)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_profit2", resByte)
			return lib.GenerateResponse(response)
		} else if grafanaQueryRequest.Targets[0].Target == "whale_sort_by_spending2" {
			var resp grafana.QueryResponse
			key := lib.GetDateRange(fromTimeObj.Unix(), time.Now().Unix())
			if body, exist := lib.GetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_spending2"); exist {
				if err := json.Unmarshal(body, &resp); err != nil {
					return lib.GenerateResponse(resp)
				}
			}
			whaleRois := daily.GetWhaleRois(h.s3Client, h.cache, schema.StarSharksStartingDate.Unix(), time.Now().Unix(), schema.SortBySpending)
			response := grafana.GetWhaleRoisMetrics(whaleRois, schema.SortBySpending)
			resByte, _ := json.Marshal(response)
			go lib.SetRangeCacheFromS3(h.s3Client, key, "whale_sort_by_spending2", resByte)
			return lib.GenerateResponse(response)
		}
		return lib.GenerateResponse("")
	}
	return lib.GenerateResponse("")
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
	s3client := s3.New(sess)
	h := handler{
		s3Client: s3client,
		cache:    lib.NewCache(),
	}
	lambda.Start(h.process)
}
