package main

import (
	"log"
	"sort"
	"time"
)

func GetUserRepurchaseRate(fromTimeObj time.Time, toTimeObj time.Time) float64 {
	totalTransfers := GetTransfers(fromTimeObj, toTimeObj)
	perUserTransfers := map[string][]Transfer{}
	repurchaseUserCount := 0
	for _, transfer := range totalTransfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
	}
	for _, transfers := range perUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		sort.Slice(transfers, func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
		if transfers[len(transfers)-1].Timestamp-transfers[0].Timestamp > dayInSec {
			repurchaseUserCount += 1
		}
	}
	log.Printf("total user count: %d, repurhase user count: %d", len(perUserTransfers), repurchaseUserCount)
	return float64(repurchaseUserCount) / float64(len(perUserTransfers))
}
