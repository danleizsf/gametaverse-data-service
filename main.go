package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"
)

func process(ctx context.Context, input Input) (interface{}, error) {
	log.Printf("intput: %v", input)
	if input.Method == "getDaus" {
		return GetGameDaus(generateTimeObjs(input)), nil
	} else if input.Method == "getDailyTransactionVolumes" {
		response := GetGameDailyTransactionVolumes(generateTimeObjs(input))
		return response, nil
	} else if input.Method == "getUserData" {
		return getUserData(input.Params[0].Address)
	} else if input.Method == "getUserRetentionRate" {
		response := GetUserRetentionRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserRepurchaseRate" {
		response := GetUserRepurchaseRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserSpendingDistribution" {
		response := getUserSpendingDistribution(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
	} else if input.Method == "getUserProfitDistribution" {
		log.Printf("input address %s", input.Params[0].Address)
		response := getUserProfitDistribution(input.Params[0].Address, time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserRoi" {
		response := getUserRoi(time.Unix(input.Params[0].FromTimestamp, 0), time.Unix(input.Params[0].ToTimestamp, 0))
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getUserActiveDates" {
		response := getUserActiveDates(starSharksStartingDate, time.Now())
		return response, nil
		//return generateJsonResponse(response)
	} else if input.Method == "getNewUserProfitableRate" {
		response := getNewUserProfitableRate(time.Unix(input.Params[0].FromTimestamp, 0), time.Now())
		return response, nil
	} else if input.Method == "getUserType" {
		response := getUserType(input.Params[0].Address)
		return response, nil
	}
	return "", nil
}

func main() {
	lambda.Start(process)
}

func getActiveUsersFromTransfers(transfers []Transfer) map[string]bool {
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transfer := range transfers {
		count += 1
		uniqueAddresses[transfer.FromAddress] = true
		uniqueAddresses[transfer.ToAddress] = true
	}
	return uniqueAddresses
}

func getUserTransactionVolume(address string, transfers []Transfer) float64 {
	transactionVolume := float64(0)
	for _, transfer := range transfers {
		if transfer.FromAddress == address || transfer.ToAddress == address {
			transactionVolume += transfer.Value
			log.Printf("address: %s, transactionHash: %s, value: %v", address, transfer.TransactionHash, transfer.Value)
		}
	}
	return transactionVolume / 1000000000000000000
}

func getTransactionVolumeFromTransfers(transfers []Transfer, timestamp int64) UserTransactionVolume {
	renterTransactionVolume, purchaserTransactionVolume, withdrawerTransactionVolume := int64(0), int64(0), int64(0)
	for _, transfer := range transfers {
		if transfer.ContractAddress == starSharksRentContractAddresses {
			renterTransactionVolume += int64(transfer.Value / float64(seaTokenUnit))
		} else if transfer.ContractAddress == starSharksPurchaseContractAddresses || transfer.ContractAddress == starSharksAuctionContractAddresses {
			purchaserTransactionVolume += int64(transfer.Value / float64(seaTokenUnit))
		} else if transfer.ContractAddress == starSharksWithdrawContractAddresses {
			withdrawerTransactionVolume += int64(transfer.Value / float64(seaTokenUnit))
		}
	}
	return UserTransactionVolume{
		RenterTransactionVolume:     renterTransactionVolume,
		PurchaserTransactionVolume:  purchaserTransactionVolume,
		WithdrawerTransactionVolume: withdrawerTransactionVolume,
	}
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func getUserData(address string) (string, error) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	dailyTransactionVolume := make(map[string]float64)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		dateTimestamp, _ := strconv.Atoi(strings.Split(*item.Key, "-")[0])
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		dateObj := time.Unix(int64(dateTimestamp), 0).UTC()
		dateFormattedString := fmt.Sprintf("%d-%d-%d", dateObj.Year(), dateObj.Month(), dateObj.Day())
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
		dailyTransactionVolume[dateFormattedString] = getUserTransactionVolume(address, transfers)
	}
	return fmt.Sprintf("{starsharks: {dailyTransactionVolume: %v SEA Token}}", dailyTransactionVolume), nil
}

func getUserSpendingDistribution(fromTimeObj time.Time, toTimeObj time.Time) []ValueFrequencyPercentage {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	totalTransfers := make([]Transfer, 0)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserSpending := getPerUserSpending(totalTransfers)

	return generateValueDistribution(perUserSpending)
}

func getPerUserSpending(transfers []Transfer) map[string]int64 {
	perUserSpending := make(map[string]int64)
	for _, transfer := range transfers {
		if _, ok := starSharksGameWalletAddresses[transfer.FromAddress]; ok {
			continue
		}
		if spending, ok := perUserSpending[transfer.FromAddress]; ok {
			perUserSpending[transfer.FromAddress] = spending + int64(transfer.Value/1000000000000000000)
		} else {
			perUserSpending[transfer.FromAddress] = int64(transfer.Value / 1000000000000000000)
		}
	}
	return perUserSpending
}

func generateValueDistribution(perUserValue map[string]int64) []ValueFrequencyPercentage {
	valueDistribution := make(map[int64]int64)
	totalFrequency := int64(0)
	for _, value := range perUserValue {
		valueDistribution[value] += 1
		totalFrequency += 1
	}
	valuePercentageDistribution := make(map[int64]float64)
	for value, frequency := range valueDistribution {
		valuePercentageDistribution[value] = float64(frequency) / float64(totalFrequency)
	}
	result := make([]ValueFrequencyPercentage, len(valuePercentageDistribution))
	idx := 0
	for value, percentage := range valuePercentageDistribution {
		result[idx] = ValueFrequencyPercentage{
			Value:               value,
			FrequencyPercentage: percentage,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Value < result[j].Value
	})
	return result
}

func isEligibleToProcess(timeObj time.Time, targetTimeObjs []time.Time) bool {
	eligibleToProcess := false
	for _, targetTimeObj := range targetTimeObjs {
		log.Printf("targetTime: %v, time: %v", targetTimeObj, timeObj)
		if targetTimeObj.Year() == timeObj.Year() && targetTimeObj.Month() == timeObj.Month() && targetTimeObj.Day() == timeObj.Day() {
			eligibleToProcess = true
			break
		}
	}
	return eligibleToProcess
}

func generateTimeObjs(input Input) []time.Time {
	times := make([]time.Time, 0)
	for _, param := range input.Params {
		if param.Timestamp != 0 {
			times = append(times, time.Unix(param.Timestamp, 0))
		}
	}
	return times
}

func getUserRoi(fromTimeObjs time.Time, toTimeObj time.Time) []ValueFrequencyPercentage {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	eligibleTransfers := make([]Transfer, 0)
	targetUsers := getNewUsers(fromTimeObjs, toTimeObj, *svc)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObjs) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		eligibleTransfers = append(eligibleTransfers, transfers...)
	}

	targetUserTransfers := map[string][]Transfer{}

	for _, transfer := range eligibleTransfers {
		if _, ok := targetUsers[transfer.FromAddress]; ok {
			if _, ok := targetUserTransfers[transfer.FromAddress]; ok {
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			} else {
				targetUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
				targetUserTransfers[transfer.FromAddress] = append(targetUserTransfers[transfer.FromAddress], transfer)
			}
		}
		if _, ok := targetUsers[transfer.ToAddress]; ok {
			if _, ok := targetUserTransfers[transfer.ToAddress]; ok {
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			} else {
				targetUserTransfers[transfer.ToAddress] = make([]Transfer, 0)
				targetUserTransfers[transfer.ToAddress] = append(targetUserTransfers[transfer.ToAddress], transfer)
			}
		}
	}

	for userAddress, transfers := range targetUserTransfers {
		sort.Slice(targetUserTransfers[userAddress], func(i, j int) bool {
			return transfers[i].Timestamp < transfers[j].Timestamp
		})
	}

	eligibleTargetUserTransfers := map[string][]Transfer{}
	for userAddress, transfers := range targetUserTransfers {
		if len(transfers) == 0 {
			continue
		}
		timeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		if timeObj.Before(fromTimeObjs) || timeObj.After(toTimeObj) {
			continue
		}
		eligibleTargetUserTransfers[userAddress] = transfers
	}

	eligibleTargetUserRoi := map[string]int64{}
	for userAddress, transfers := range eligibleTargetUserTransfers {
		value := -1
		transferIdx := -1
		for _, transfer := range transfers {
			if transfer.FromAddress == userAddress {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("spend %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value -= int(transfer.Value / 1000000000000000000)
			} else {
				//if userAddress == "0xf9d207589d17f5512d367aafba7e81042a89ba3e" {
				//	log.Printf("earn %d, total %d", int(transfer.Value/1000000000000000000), value)
				//}
				value += int(transfer.Value / 1000000000000000000)
			}
			transferIdx += 1
			if value > 0 {
				break
			}
		}

		if value < 0 {
			continue
		}

		initialTransferTimeObj := time.Unix(int64(transfers[0].Timestamp), 0)
		profitTransferTimeObj := time.Unix(int64(transfers[transferIdx].Timestamp), 0)
		eligibleTargetUserRoi[userAddress] = int64(math.Ceil(profitTransferTimeObj.Sub(initialTransferTimeObj).Hours() / 24))
	}

	return generateRoiDistribution(eligibleTargetUserRoi)
}

func generateRoiDistribution(perUserRoiInDays map[string]int64) []ValueFrequencyPercentage {
	RoiDayDistribution := make(map[int64]int64)
	totalCount := float64(0)
	for _, days := range perUserRoiInDays {
		if days < 1 {
			continue
		}
		RoiDayDistribution[days] += 1
		totalCount += 1
	}
	daysPercentageDistribution := make(map[int64]float64)
	for days, count := range RoiDayDistribution {
		daysPercentageDistribution[days] = float64(count) / totalCount
	}
	result := make([]ValueFrequencyPercentage, len(daysPercentageDistribution))
	idx := 0
	for value, frequencyPercentage := range daysPercentageDistribution {
		result[idx] = ValueFrequencyPercentage{
			Value:               value,
			FrequencyPercentage: frequencyPercentage,
		}
		idx += 1
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Value < result[j].Value
	})
	return result
}

func getNewUsers(fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) map[string]int64 {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(userBucketName),
			Key:    aws.String("per-user-join-time.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	newUsers := map[string]int64{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		userJoinTimestampObj := time.Unix(int64(timestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		newUsers[address] = int64(timestamp)
	}
	return newUsers
}

func getPriceHistory(tokenName string, fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) PriceHistory {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(priceBucketName),
			Key:    aws.String("sea-token-price-history.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Unable to read body, %v", err)
	}

	priceHistory := PriceHistory{}
	err = json.Unmarshal(body, &priceHistory)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		exitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	return priceHistory
}

func GetUserRepurchaseRate(fromTimeObj time.Time, toTimeObj time.Time) float64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
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

func getUserActiveDates(fromTimeObj time.Time, toTimeObj time.Time) []UserActivity {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perUserTransfers := map[string][]Transfer{}
	for _, transfer := range totalTransfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
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

func getNewUserProfitableRate(fromTimeObj time.Time, toTimeObj time.Time) AllUserRoiDetails {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	newUsers := getNewUsers(fromTimeObj, toTimeObj, *svc)

	totalTransfers := make([]Transfer, 0)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}
	perNewUserRoiDetail := map[string]*UserRoiDetail{}

	priceHistory := getPriceHistory("sea", fromTimeObj, toTimeObj, *svc)
	priceHisoryMap := map[int64]float64{}
	layout := "2006-01-02"
	for _, price := range priceHistory.Prices {
		timeObj, _ := time.Parse(layout, price.Date)
		priceHisoryMap[timeObj.Unix()] = price.Price
	}
	for _, transfer := range totalTransfers {
		//if transfer.FromAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" && transfer.ToAddress != "0xfff5de86577b3f778ac6cc236384ed6db1825bff" {
		//	continue
		//}

		//log.Printf("user %s transfer %v", "0xfff5de86577b3f778ac6cc236384ed6db1825bff", transfer)
		if joinedTimestamp, ok := newUsers[transfer.FromAddress]; ok {
			dateTimestamp := (joinedTimestamp / int64(dayInSec)) * int64(dayInSec)
			value := (transfer.Value / float64(seaTokenUnit)) * priceHisoryMap[dateTimestamp]
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.FromAddress]; ok {
				userRoiDetails.TotalProfit -= value
				userRoiDetails.TotalSpending += value
			} else {
				perNewUserRoiDetail[transfer.FromAddress] = &UserRoiDetail{
					UserAddress:       transfer.FromAddress,
					JoinDateTimestamp: joinedTimestamp,
					TotalSpending:     value,
					TotalProfit:       value,
				}
			}
		}
		if joinedTimestamp, ok := newUsers[transfer.ToAddress]; ok {
			value := transfer.Value / float64(seaTokenUnit)
			if userRoiDetails, ok := perNewUserRoiDetail[transfer.ToAddress]; ok {
				userRoiDetails.TotalProfit += value
			} else {
				perNewUserRoiDetail[transfer.ToAddress] = &UserRoiDetail{
					UserAddress:       transfer.ToAddress,
					JoinDateTimestamp: joinedTimestamp,
					TotalSpending:     0,
					TotalProfit:       value,
				}
			}
		}
	}
	userRoiDetails := make([]UserRoiDetail, len(perNewUserRoiDetail))
	profitableUserCount := 0
	idx := 0
	for _, userRoiDetail := range perNewUserRoiDetail {
		userRoiDetails[idx] = *userRoiDetail
		idx += 1
		if userRoiDetail.TotalProfit > 0 {
			profitableUserCount += 1
		}
	}

	//log.Printf("priceHistory: %v", priceHistory)
	//return AllUserRoiDetails{}
	return AllUserRoiDetails{
		UserRoiDetails:        userRoiDetails,
		OverallProfitableRate: float64(profitableUserCount) / float64(len(perNewUserRoiDetail)),
	}
}

func getUserProfitDistribution(userAddress string, fromTimeObj time.Time, toTimeObj time.Time) UserRoiDetail {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	totalTransfers := make([]Transfer, 0)

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(dailyTransferBucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		timestamp, _ := strconv.ParseInt(strings.Split(*item.Key, "-")[0], 10, 64)
		timeObj := time.Unix(timestamp, 0)
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(dailyTransferBucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		totalTransfers = append(totalTransfers, transfers...)
	}

	userRoiDetail := UserRoiDetail{}
	for _, transfer := range totalTransfers {
		if transfer.FromAddress == userAddress {
			userRoiDetail.TotalSpending += transfer.Value / float64(seaTokenUnit)
			userRoiDetail.TotalProfit -= transfer.Value / float64(seaTokenUnit)
		} else if transfer.ToAddress == userAddress {
			userRoiDetail.TotalProfit += transfer.Value / float64(seaTokenUnit)
		}
	}

	userRoiDetail.UserAddress = userAddress
	return userRoiDetail
}

func getPerPayerTransfers(transfers []Transfer) map[string][]Transfer {
	perUserTransfers := map[string][]Transfer{}
	for _, transfer := range transfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]Transfer, 0)
		}
	}
	return perUserTransfers
}

func getUserType(userAddress string) UserType {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	})

	svc := s3.New(sess)

	bucketName := "gametaverse-bucket"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
	if err != nil {
		exitErrorf("Unable to list object, %v", err)
	}

	totalTransfers := make([]Transfer, 0)
	for _, item := range resp.Contents {
		log.Printf("file name: %s\n", *item.Key)
		requestInput :=
			&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(*item.Key),
			}
		result, err := svc.GetObject(requestInput)
		if err != nil {
			exitErrorf("Unable to get object, %v", err)
		}
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			exitErrorf("Unable to get body, %v", err)
		}
		bodyString := string(body)
		//transactions := converCsvStringToTransactionStructs(bodyString)
		transfers := ConvertCsvStringToTransferStructs(bodyString)
		log.Printf("transfer num: %d", len(transfers))
		totalTransfers = append(totalTransfers, transfers...)
		//dateString := time.Unix(int64(dateTimestamp), 0).UTC().Format("2006-January-01")
		//daus[dateFormattedString] = getDauFromTransactions(transactions, int64(dateTimestamp))
	}
	payerTransfers := map[string][]Transfer{}

	for _, transfer := range totalTransfers {
		if transfer.FromAddress == userAddress || transfer.ToAddress == userAddress {
			payerTransfers[userAddress] = append(payerTransfers[userAddress], transfer)
		}
	}
	//perUserTransfers := getActiveUsersFromTransfers(transfers)
	payerType := GetPerPayerType(payerTransfers)[userAddress]
	transfers := payerTransfers[userAddress]
	if payerType == Renter {
		return UserType{
			UserAddress: userAddress,
			Type:        "renter",
			Transfers:   transfers,
		}
	} else {
		return UserType{
			UserAddress: userAddress,
			Type:        "purchaser",
			Transfers:   transfers,
		}
	}
}
