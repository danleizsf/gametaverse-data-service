package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"

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
