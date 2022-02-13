package main

import (
	"sort"
	"time"
)

func GetUserActiveDates(fromTimeObj time.Time, toTimeObj time.Time) []UserActivity {

	totalTransfers := GetTransfers(fromTimeObj, toTimeObj)
	//for _, item := range resp.Contents {
	//	log.Printf("file name: %s\n", *item.Key)
	//	timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
	//	timeObj := time.Unix(timestamp, 0)
	//	if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
	//		continue
	//	}

	//	requestInput :=
	//		&s3.GetObjectInput{
	//			Bucket: aws.String(dailyTransferBucketName),
	//			Key:    aws.String(*item.Key),
	//		}
	//	result, err := svc.GetObject(requestInput)
	//	if err != nil {
	//		exitErrorf("Unable to get object, %v", err)
	//	}
	//	body, err := ioutil.ReadAll(result.Body)
	//	if err != nil {
	//		exitErrorf("Unable to get body, %v", err)
	//	}
	//	bodyString := string(body)
	//	//transactions := converCsvStringToTransactionStructs(bodyString)
	//	transfers := ConvertCsvStringToTransferStructs(bodyString)
	//	log.Printf("transfer num: %d", len(transfers))
	//	//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
	//	totalTransfers = append(totalTransfers, transfers...)
	//}
	perUserTransfers := map[string][]Transfer{}
	for _, transfer := range totalTransfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
		if _, ok := perUserTransfers[transfer.ToAddress]; ok {
			perUserTransfers[transfer.ToAddress] = append(perUserTransfers[transfer.ToAddress], transfer)
		} else {
			perUserTransfers[transfer.ToAddress] = make([]Transfer, 0)
		}
	}
	perUserActivities := make([]UserActivity, 0) //len(perUserTransfers))
	idx := 0
	for userAddress, transfers := range perUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		sort.Slice(transfers, func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
		totalDatesCount := transfers[len(transfers)-1].Timestamp/dayInSec - transfers[0].Timestamp/dayInSec + 1
		activeDatesCount := 1
		currentDate := transfers[0].Timestamp / dayInSec
		for _, transfer := range transfers {
			if transfer.Timestamp/dayInSec != currentDate {
				activeDatesCount += 1
				currentDate = transfer.Timestamp / dayInSec
			}
		}

		//if userAddress == "0x27eafaf87860c290c185c1105cbedeb3b742c748" {
		//	log.Printf("for user %s, totalDatesCount %d, activeDatesCount %d", userAddress, totalDatesCount, activeDatesCount)
		//	for _, transfer := range transfers {
		//		log.Printf("transfer timestamp %d, date %d", transfer.Timestamp, transfer.Timestamp/dayInSec)
		//	}
		//	perUserActivities[idx] = UserActivity{UserAddress: userAddress, TotalDatesCount: int64(totalDatesCount), ActiveDatesCount: int64(activeDatesCount)}
		//}
		perUserActivities = append(perUserActivities, UserActivity{UserAddress: userAddress, TotalDatesCount: int64(totalDatesCount), ActiveDatesCount: int64(activeDatesCount)})
		idx += 1
	}
	sort.Slice(perUserActivities, func(i, j int) bool {
		return perUserActivities[i].TotalDatesCount > perUserActivities[j].TotalDatesCount
	})
	return perUserActivities
}
