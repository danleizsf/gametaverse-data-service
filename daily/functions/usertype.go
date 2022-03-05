package daily

import (
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"

	"github.com/aws/aws-sdk-go/service/s3"
)

func GetUserType(s3client *s3.S3, cache *Cache, timestampA int64, timestampB int64) schema.UserTypeCount {
	useractions := lib.GetUserActionsRangeAsync(s3client, cache, timestampA, timestampB)
	return GetUserTypeWithUserActions(useractions)
}

func GetUserTypeWithUserActions(useractions map[string][]schema.UserAction) schema.UserTypeCount {
	rentees := map[string]bool{}
	purchasers := map[string]bool{}
	hybrids := map[string]bool{}
	others := map[string]bool{}

	for user, actions := range useractions {
		userType := UserType(actions)
		switch userType {
		case schema.Rentee:
			rentees[user] = true
		case schema.Purchaser:
			purchasers[user] = true
		case schema.Hybrider:
			hybrids[user] = true
		default:
			others[user] = true
		}
	}
	return schema.UserTypeCount{
		RenteeCount:    int64(len(rentees)),
		PurchaserCount: int64(len(purchasers)),
		HybridCount:    int64(len(hybrids)),
		OtherCount:     int64(len(others)),
	}
}

func UserType(actions []schema.UserAction) schema.PayerType {
	rent, purchase, earning := 0, 0, 0
	for _, a := range actions {
		if a.Action == schema.UserActionAuctionBuyNFT || a.Action == schema.UserActionBuyNFT {
			purchase++
		} else if a.Action == schema.UserActionRentSharkSEA {
			rent++
		} else if a.Action == schema.UserActionAuctionSellNFT || a.Action == schema.UserActionLendSharkSEA {
			earning++
		}
	}
	if rent != 0 && purchase != 0 {
		return schema.Hybrider
	}
	if rent != 0 {
		return schema.Rentee
	} else if purchase != 0 {
		return schema.Purchaser
	} else {
		return schema.Unknown
	}

}
