package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"sort"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetNewUserProfitableRate(s3client *s3.S3, cache *lib.Cache, timestampA int64, timestampB int64, forDebug bool) schema.AllUserRoiDetails {
	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	perNewUserRoiDetail := map[string]*schema.UserRoiDetail{}

	priceHistory := lib.GetPriceHistoryV2(s3client)
	priceHisoryMap := map[string]float64{}
	for _, price := range priceHistory.Prices {
		priceHisoryMap[price.Date] = price.Price
	}
	for user, actions := range useractions {
		userType := UserType(actions)
		var spendingToken, gainToken, usdSpending, usdGain float64
		for _, a := range actions {
			if a.Action == schema.UserActionRentSharkSEA || a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
				spendingToken += a.Value.(float64)
			} else if a.Action == schema.UserActionLendSharkSEA || a.Action == schema.UserActionAuctionSellSEA || a.Action == schema.UserActionWithdrawlSEA {
				gainToken += a.Value.(float64)
			}
			price := priceHisoryMap[a.Date]
			usdSpending += spendingToken * price
			usdGain += gainToken * price
		}

		perNewUserRoiDetail[user] = &schema.UserRoiDetail{
			UserAddress:        user,
			JoinDate:           actions[0].Date,
			TotalSpendingUsd:   usdSpending,
			TotalProfitUsd:     usdGain - usdSpending,
			TotalSpendingToken: spendingToken,
			TotalProfitToken:   gainToken - spendingToken,
			UserType:           userType,
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
	uas := make(map[string][]schema.UserAction, len(resp))
	for _, roi := range resp {
		uas[roi.UserAddress] = useractions[roi.UserAddress]
	}

	lib.ToFile(resp, "roi.json")
	lib.ToFile(uas, "ua.json")
	return resp
}
