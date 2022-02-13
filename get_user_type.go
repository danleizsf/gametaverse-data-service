package main

import (
	"time"
)

func GetUserType() UserTypeCount {
	fromTimeObj := starSharksStartingDate
	toTimeObj := time.Now()
	totalTransfers := GetTransfers(fromTimeObj, toTimeObj)
	rentees := map[string]bool{}
	purchasers := map[string]bool{}
	hybrids := map[string]bool{}

	for _, transfer := range totalTransfers {
		payerAddress := transfer.FromAddress
		if _, ok := hybrids[payerAddress]; ok {
			continue
		}
		if transfer.ContractAddress == starSharksRentContractAddresses {
			if _, ok := purchasers[payerAddress]; ok {
				delete(rentees, payerAddress)
				hybrids[payerAddress] = true
			} else if _, ok := rentees[payerAddress]; !ok {
				rentees[payerAddress] = true
			}
		}
		if transfer.ContractAddress == starSharksPurchaseContractAddresses || transfer.ContractAddress == starSharksAuctionContractAddresses {
			if _, ok := rentees[payerAddress]; ok {
				delete(purchasers, payerAddress)
				hybrids[payerAddress] = true
			} else if _, ok := purchasers[payerAddress]; !ok {
				purchasers[payerAddress] = true
			}
		}
	}
	return UserTypeCount{
		RenteeCount:    int64(len(rentees)),
		PurchaserCount: int64(len(purchasers)),
		HybridCount:    int64(len(hybrids)),
	}
}
