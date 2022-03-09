package daily

import (
	"encoding/json"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserProfitableRate(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64, forDebug bool, fromTimeObj time.Time, toTimeObj time.Time) schema.AllUserRoiDetails {
	functionName := "GetNewUserProfitableRate"
	if forDebug {
		functionName = "GetNewUserProfitableRateDebug"
	}
	var resp schema.AllUserRoiDetails

	key := lib.GetDateRange(timestampA, timestampB)
	if body, exist := lib.GetRangeCacheFromS3(s3client, key, functionName); exist {
		json.Unmarshal(body, &resp)
		return resp
	}

	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	perNewUserRoiDetail := map[string]*schema.UserRoiDetail{}

	priceHistory := lib.GetPriceHistoryV2(s3client)
	priceHisoryMap := map[string]float64{}
	for _, price := range priceHistory.Prices {
		priceHisoryMap[price.Date] = price.Price
	}
	for user, actions := range useractions {
		userType := UserType(actions)
		var totalSpendingToken, totalGainToken, totalSpendingUSD, totalGainUSD float64
		for _, a := range actions {
			var spendingToken, gainToken float64
			if a.Action == schema.UserActionRentSharkSEA || a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				spendingToken = a.Value.(float64)
				totalSpendingToken += spendingToken
			} else if a.Action == schema.UserActionLendSharkSEA || a.Action == schema.UserActionAuctionSellSEA || a.Action == schema.UserActionWithdrawlSEA {
				gainToken = a.Value.(float64)
				totalGainToken += gainToken
			}
			price := priceHisoryMap[a.Date]
			if spendingToken > 0 {
				totalSpendingUSD += spendingToken * price
			}
			if gainToken > 0 {
				totalGainUSD += gainToken * price
			}
		}

		perNewUserRoiDetail[user] = &schema.UserRoiDetail{
			UserAddress:        user,
			JoinDate:           actions[0].Date,
			TotalGainToken:     totalGainToken,
			TotalGainUsd:       totalGainUSD,
			TotalSpendingToken: totalSpendingToken,
			TotalSpendingUsd:   totalSpendingUSD,
			TotalProfitToken:   totalGainToken - totalSpendingToken,
			TotalProfitUsd:     totalGainUSD - totalSpendingUSD,
			UserType:           userType,
		}
	}

	starSharksMysteriousBoxTransfers := lib.GetMysteriousBoxTransfers(fromTimeObj, toTimeObj, s3client)
	for _, transfer := range starSharksMysteriousBoxTransfers {
		date := time.Unix(int64(transfer.Timestamp), 0).Format(schema.DateFormat)
		address := transfer.FromAddress
		valueToken := transfer.Value / float64(schema.SeaTokenUnit)
		valueUsd := valueToken * priceHisoryMap[date]
		if _, ok := perNewUserRoiDetail[address]; ok {
			perNewUserRoiDetail[address].TotalProfitToken -= valueToken
			perNewUserRoiDetail[address].TotalProfitUsd -= valueUsd
			perNewUserRoiDetail[address].TotalSpendingToken += valueToken
			perNewUserRoiDetail[address].TotalSpendingUsd += valueUsd
		}
	}
	userRoiDetails := make([]schema.UserRoiDetail, len(perNewUserRoiDetail))
	profitableUserCount := 0
	idx := 0
	for _, userRoiDetail := range perNewUserRoiDetail {
		userRoiDetails[idx] = *userRoiDetail
		idx += 1
		if userRoiDetail.TotalProfitUsd > 0 {
			profitableUserCount += 1
		}
	}
	response := schema.AllUserRoiDetails{
		OverallProfitableRate: float64(profitableUserCount) / float64(len(perNewUserRoiDetail)),
	}
	if forDebug {
		response.UserRoiDetails = userRoiDetails
	}
	body, _ := json.Marshal(response)
	go lib.SetRangeCacheFromS3(s3client, key, functionName, body)
	return response
}

func GetWhaleRois(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64, sortType schema.WhalesSortType) []schema.UserRoiDetail {
	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	perNewUserRoiDetail := map[string]*schema.UserRoiDetail{}
	priceHistory := lib.GetPriceHistoryV2(s3client)
	priceHisoryMap := map[string]float64{}
	for _, price := range priceHistory.Prices {
		priceHisoryMap[price.Date] = price.Price
	}
	for user, actions := range useractions {
		userType := UserType(actions)
		var totalSpendingToken, totalGainToken, totalSpendingUSD, totalGainUSD float64
		for _, a := range actions {
			var spendingToken, gainToken float64
			if a.Action == schema.UserActionRentSharkSEA || a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				spendingToken = a.Value.(float64)
				totalSpendingToken += spendingToken
			} else if a.Action == schema.UserActionLendSharkSEA || a.Action == schema.UserActionAuctionSellSEA || a.Action == schema.UserActionWithdrawlSEA {
				gainToken = a.Value.(float64)
				totalGainToken += gainToken
			}
			price := priceHisoryMap[a.Date]
			if spendingToken > 0 {
				totalSpendingUSD += spendingToken * price
			}
			if gainToken > 0 {
				totalGainUSD += gainToken * price
			}
		}

		perNewUserRoiDetail[user] = &schema.UserRoiDetail{
			UserAddress:        user,
			JoinDate:           actions[0].Date,
			TotalGainToken:     totalGainToken,
			TotalGainUsd:       totalGainUSD,
			TotalSpendingToken: totalSpendingToken,
			TotalSpendingUsd:   totalSpendingUSD,
			TotalProfitToken:   totalGainToken - totalSpendingToken,
			TotalProfitUsd:     totalGainUSD - totalSpendingUSD,
			UserType:           userType,
		}
	}
	userRoiDetails := make([]schema.UserRoiDetail, len(perNewUserRoiDetail))
	idx := 0
	for _, userRoiDetail := range perNewUserRoiDetail {
		userRoiDetails[idx] = *userRoiDetail
		idx += 1
	}

	if sortType == schema.SortByGain {
		sort.Slice(userRoiDetails, func(i, j int) bool {
			return userRoiDetails[i].TotalGainToken > userRoiDetails[j].TotalGainToken
		})
	} else if sortType == schema.SortByProfit {
		sort.Slice(userRoiDetails, func(i, j int) bool {
			return userRoiDetails[i].TotalProfitToken > userRoiDetails[j].TotalProfitToken
		})
	} else if sortType == schema.SortBySpending {
		sort.Slice(userRoiDetails, func(i, j int) bool {
			return userRoiDetails[i].TotalSpendingToken > userRoiDetails[j].TotalSpendingToken
		})
	}
	resp := userRoiDetails[0:10]
	return resp
}
