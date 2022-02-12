package main

import (
	"log"
	"strconv"
	"strings"
)

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

func ConvertCsvStringToTransferStructs(csvString string) []Transfer {
	lines := strings.Split(csvString, "\n")
	transfers := make([]Transfer, 0)
	count := 0
	log.Printf("enterred converCsvStringToTransferStructs, content len: %d", len(lines))
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 8 {
			continue
		}
		token_address := fields[0]
		if token_address != "0x26193c7fa4354ae49ec53ea2cebc513dc39a10aa" {
			continue
		}
		count += 1
		timestamp, _ := strconv.Atoi(fields[7])
		blockNumber, _ := strconv.Atoi(fields[6])
		value, _ := strconv.ParseFloat(fields[3], 64)
		logIndex, _ := strconv.Atoi(fields[5])
		transfers = append(transfers, Transfer{
			TokenAddress:    fields[0],
			FromAddress:     fields[1],
			ToAddress:       fields[2],
			Value:           value,
			TransactionHash: fields[4],
			LogIndex:        logIndex,
			BlockNumber:     blockNumber,
			Timestamp:       timestamp,
			ContractAddress: fields[8],
		})
	}
	return transfers
}
