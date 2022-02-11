package main

func GetPerPayerType(perPayerTransfers map[string][]Transfer) map[string]payerType {
	perPayerType := map[string]payerType{}
	for payerAddress, transfers := range perPayerTransfers {
		totalRentingValue := float64(0)
		totalInvestingValue := float64(0)
		for _, transfer := range transfers {
			if transfer.ContractAddress == starSharksPurchaseContractAddresses || transfer.ContractAddress == starSharksAuctionContractAddresses {
				totalInvestingValue += transfer.Value / float64(dayInSec)
			} else if transfer.ContractAddress == starSharksRentContractAddresses {
				totalRentingValue += transfer.Value / float64(dayInSec)
			}
		}
		if totalInvestingValue > totalRentingValue {
			perPayerType[payerAddress] = Purchaser
		} else {
			perPayerType[payerAddress] = Renter
		}
	}
	return perPayerType
}
