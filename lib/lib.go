package lib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gametaverse-data-service/schema"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	DailyBucketName = "gametaverse-daily-starsharks"
)

func GetDate(timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC() //UTC returns t with the location set to UTC.
	return t.Format("2006-01-02")
}

func getSummary(s3client *s3.S3, date string) schema.Summary {
	var s schema.Summary
	summaryRequest := &s3.GetObjectInput{
		Bucket: aws.String(DailyBucketName),
		Key:    aws.String(date + "/summary.json"),
	}
	data, err := s3client.GetObject(summaryRequest)
	if err != nil {
		log.Print("cannot get summary for date: " + date)
		return s
	}
	body, err := ioutil.ReadAll(data.Body)
	if err != nil {
		return s
	}
	err = json.Unmarshal(body, &s)
	if err != nil {
		log.Print("can't unmarshall object " + *summaryRequest.Key)
	}
	return s
}

func GetRangeCacheFromS3(s3client *s3.S3, key string, functionName string) ([]byte, bool) {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(DailyBucketName),
			Key:    aws.String("cache/" + key + "/" + functionName),
		}
	result, err := s3client.GetObject(requestInput)
	if err != nil {
		log.Printf("Unable to get object, key: %s, func:  %s, %v", key, functionName, err)
		return nil, false
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		log.Printf("Unable to get body, %v", err)
		return nil, false
	}
	log.Printf("S3 cache hit for key: %s, function: %s", key, functionName)
	return body, true
}

func SetRangeCacheFromS3(s3client *s3.S3, key string, functionName string, body []byte) {
	requestInput :=
		&s3.PutObjectInput{
			Bucket: aws.String(DailyBucketName),
			Key:    aws.String("cache/" + key + "/" + functionName),
			Body:   bytes.NewReader(body),
		}
	_, err := s3client.PutObject(requestInput)
	if err != nil {
		log.Printf("Unable to put object, %v", err)
	}
	log.Printf("Upload S3 cache for key: %s, function: %s", key, functionName)
}

func GetSignatureRangesBefore(date time.Time) []time.Time {
	resp := make([]time.Time, 4)
	resp[0] = date.AddDate(
		0, 0, -7,
	)
	resp[1] = date.AddDate(
		0, 0, -30,
	)
	resp[2] = date.AddDate(
		0, -1, 0,
	)
	resp[3] = schema.StarSharksStartingDate
	return resp
}

func GetSignatureRangesAfter(date time.Time) []time.Time {
	resp := make([]time.Time, 4)
	resp[0] = date.AddDate(
		0, 0, 7,
	)
	resp[1] = date.AddDate(
		0, 0, 30,
	)
	resp[2] = date.AddDate(
		0, 1, 0,
	)
	resp[3] = time.Now()
	return resp
}

// func UploadRangeToS3(s3client *s3.S3, ua map[string][]schema.UserAction, timestampA int64, timestampB int64) {
// 	start := time.Unix(timestampA, 0)
// 	end := time.Unix(timestampB, 0)
// 	endTimes := GetSignatureRangesAfter(start)
// 	now := time.Now()
// 	for _, endTime := range endTimes {
// 		if endTime.Before(now) {
// 			cacheKey := start.Format(schema.DateFormat) + "-" + endTime.Format(schema.DateFormat)
// 			SetRangeCacheFromS3(s3client, cacheKey, ua)
// 		}
// 	}

// 	startTimes := GetSignatureRangesBefore(end)
// 	for _, startTime := range startTimes {
// 		if startTime.After(schema.StarSharksStartingDate) {
// 			cacheKey := startTime.Format(schema.DateFormat) + "-" + end.Format(schema.DateFormat)
// 			SetRangeCacheFromS3(s3client, cacheKey, ua)
// 		}
// 	}
// }

func GetDateRange(timestampA int64, timestampB int64) string {
	start := time.Unix(timestampA, 0)
	if start.Before(schema.StarSharksStartingDate) {
		start = schema.StarSharksStartingDate
	}
	end := time.Unix(timestampB, 0)
	if end.After(time.Now()) {
		end = time.Now()
	}
	return start.Format(schema.DateFormat) + "-" + end.Format(schema.DateFormat)
}

func GetUserActionsRangeAsync(s3client *s3.S3, cache *Cache, timestampA int64, timestampB int64) map[string][]schema.UserAction {
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)

	cacheKey := start.Format(schema.DateFormat) + "-" + end.Format(schema.DateFormat)
	if resp, exists := cache.GetUA(cacheKey); exists {
		return resp
	}
	// resp, exists := GetRangeCacheFromS3(s3client, cacheKey)
	// if exists {
	// 	return resp
	// }
	length := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		length++
	}
	concurrentUserActions := make([]map[string][]schema.UserAction, length+1)

	var wg sync.WaitGroup
	wg.Add(length)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format(schema.DateFormat)
			uas := getUserActions(s3client, date)
			concurrentUserActions[i] = uas
		}(i, s3client, d)
		i++
	}
	wg.Wait()
	userActions := make(map[string][]schema.UserAction, 0)
	for _, dailyUserActions := range concurrentUserActions {
		for user, actions := range dailyUserActions {
			if ua, exists := userActions[user]; exists {
				userActions[user] = append(ua, actions...)
			} else {
				userActions[user] = actions
			}
		}
	}
	cache.AddUA(cacheKey, userActions)
	// go UploadRangeToS3(s3client, userActions, timestampA, timestampB)
	return userActions
}

func GetUserActionsRangeAsyncByDate(s3client *s3.S3, cache *Cache, timestampA int64, timestampB int64) []map[string][]schema.UserAction {
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)

	cacheKey := start.Format(schema.DateFormat) + "-" + end.Format(schema.DateFormat)
	if resp, exists := cache.GetUAByDate(cacheKey); exists {
		return resp
	}
	length := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		length++
	}
	concurrentUserActions := make([]map[string][]schema.UserAction, length+1)

	var wg sync.WaitGroup
	wg.Add(length)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format(schema.DateFormat)
			uas := getUserActions(s3client, date)
			concurrentUserActions[i] = uas
		}(i, s3client, d)
		i++
	}
	wg.Wait()
	userActions := make([]map[string][]schema.UserAction, len(concurrentUserActions))
	for i, dailyUserActions := range concurrentUserActions {
		dailyUserActionMap := make(map[string][]schema.UserAction)
		for user, actions := range dailyUserActions {
			if ua, exists := dailyUserActionMap[user]; exists {
				dailyUserActionMap[user] = append(ua, actions...)
			} else {
				dailyUserActionMap[user] = actions
			}
		}
		userActions[i] = dailyUserActionMap
	}
	cache.AddUAByDate(cacheKey, userActions)
	return userActions
}

func GetSummaryRangeAsync(s3client *s3.S3, cache *Cache, timestampA int64, timestampB int64) []schema.Summary {
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)

	cacheKey := start.Format(schema.DateFormat) + "-" + end.Format(schema.DateFormat)
	if resp, exists := cache.GetSummary(cacheKey); exists {
		return resp
	}
	length := 0
	idxToDate := map[int]time.Time{}
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		idxToDate[length] = d
		length++
	}
	concurrentSummary := make([]schema.Summary, length+1)

	var wg sync.WaitGroup
	wg.Add(length)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(i int, s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format(schema.DateFormat)
			s := getSummary(s3client, date)
			concurrentSummary[i] = s
		}(i, s3client, d)
		i++
	}
	wg.Wait()

	cache.AddSummary(cacheKey, concurrentSummary)
	return concurrentSummary
}

func getUserActions(s3client *s3.S3, date string) map[string][]schema.UserAction {
	var s map[string][]schema.UserAction
	req := &s3.GetObjectInput{
		Bucket: aws.String(DailyBucketName),
		Key:    aws.String(date + "/user_actions.json"),
	}
	data, err := s3client.GetObject(req)
	if err != nil || data == nil || data.Body == nil {
		log.Printf("can't find object. %s, %v ", *req.Key, err)
		return nil
	}
	body, err := ioutil.ReadAll(data.Body)
	if err != nil {
		log.Print("can't read object " + *req.Key)
	}
	body = bytes.Replace(body, []byte(":NaN"), []byte(":null"), -1)
	err = json.Unmarshal(body, &s)
	if err != nil {
		log.Print("can't unmarshall object " + *req.Key)
	}

	for user, actions := range s {
		actionsWithDate := make([]schema.UserAction, len(actions))
		for i, a := range actions {
			actionsWithDate[i] = schema.UserAction{
				Value:  a.Value,
				Date:   date,
				Action: a.Action,
			}
		}
		s[user] = actionsWithDate
	}
	return s
}

func GetPerPayerType(perPayerTransfers map[string][]schema.Transfer) map[string]schema.PayerType {
	perPayerType := map[string]schema.PayerType{}
	for payerAddress, transfers := range perPayerTransfers {
		totalRentingValue := float64(0)
		totalInvestingValue := float64(0)
		for _, transfer := range transfers {
			if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {
				totalInvestingValue += transfer.Value / float64(schema.DayInSec)
			} else if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
				totalRentingValue += transfer.Value / float64(schema.DayInSec)
			}
		}
		if totalInvestingValue > totalRentingValue {
			perPayerType[payerAddress] = schema.Purchaser
		} else {
			perPayerType[payerAddress] = schema.Rentee
		}
	}
	return perPayerType
}

func ToFile(data interface{}, fileName string) {
	s, _ := json.Marshal(data)
	f, err := os.Create(fileName)

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	_, err2 := f.WriteString(string(s))
	if err2 != nil {
		log.Fatal(err2)
	}
}

func GetActiveUsersFromTransfers(transfers []schema.Transfer) map[string]bool {
	uniqueAddresses := make(map[string]bool)
	count := 0
	for _, transfer := range transfers {
		count += 1
		uniqueAddresses[transfer.FromAddress] = true
		uniqueAddresses[transfer.ToAddress] = true
	}
	return uniqueAddresses
}

func ExitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func GetPerUserSpending(transfers []schema.Transfer) map[string]int64 {
	perUserSpending := make(map[string]int64)
	for _, transfer := range transfers {
		if _, ok := schema.StarSharksGameWalletAddresses[transfer.FromAddress]; ok {
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

func GenerateValueDistribution(perUserValue map[string]int64) []schema.ValueFrequencyPercentage {
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
	result := make([]schema.ValueFrequencyPercentage, len(valuePercentageDistribution))
	idx := 0
	for value, percentage := range valuePercentageDistribution {
		result[idx] = schema.ValueFrequencyPercentage{
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

func GetNewUsers(fromTimeObj time.Time, toTimeObj time.Time, svc s3.S3) map[string]int64 {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.UserBucketName),
			Key:    aws.String("per-user-join-time.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		ExitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		ExitErrorf("Unable to read body, %v", err)
	}

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		ExitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	newUsers := map[string]int64{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		userJoinTimestampObj := time.Unix(int64(timestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		if _, ok := schema.StarSharksGameWalletAddresses[address]; ok {
			continue
		}
		newUsers[address] = int64(timestamp)
	}
	return newUsers
}

func GetAllTimeNewUsers(svc s3.S3) map[string]int64 {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.UserBucketName),
			Key:    aws.String("per-user-join-time.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		ExitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		ExitErrorf("Unable to read body, %v", err)
	}

	m := map[string]map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
		ExitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	newUsers := map[string]int64{}
	for address, userMetaInfo := range m {
		timestamp, _ := strconv.Atoi(userMetaInfo["timestamp"])
		if _, ok := schema.StarSharksGameWalletAddresses[address]; ok {
			continue
		}
		newUsers[address] = int64(timestamp)
	}
	return newUsers
}

func ExtractNewUsersForTimeRange(allTimeNewUsers map[string]int64, fromTimeObj time.Time, toTimeObj time.Time) map[string]int64 {
	newUsers := map[string]int64{}
	for address, joinTimestamp := range allTimeNewUsers {
		userJoinTimestampObj := time.Unix(int64(joinTimestamp), 0)
		if userJoinTimestampObj.Before(fromTimeObj) || userJoinTimestampObj.After(toTimeObj) {
			continue
		}
		newUsers[address] = int64(joinTimestamp)
	}
	return newUsers
}

func GetPriceHistoryV2(svc *s3.S3) schema.PriceHistory {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String("gametaverse-daily-starsharks"),
			Key:    aws.String("sea-token-price-history.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		ExitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		ExitErrorf("Unable to read body, %v", err)
	}

	priceHistory := schema.PriceHistory{}
	err = json.Unmarshal(body, &priceHistory)
	if err != nil {
		log.Printf("body: %s", fmt.Sprintf("%s", body))
		ExitErrorf("Unable to unmarshall user meta info, %v", err)
	}

	return priceHistory
}
func GetPerPayerTransfers(transfers []schema.Transfer) map[string][]schema.Transfer {
	perUserTransfers := map[string][]schema.Transfer{}
	for _, transfer := range transfers {
		if _, ok := perUserTransfers[transfer.FromAddress]; ok {
			perUserTransfers[transfer.FromAddress] = append(perUserTransfers[transfer.FromAddress], transfer)
		} else {
			perUserTransfers[transfer.FromAddress] = make([]schema.Transfer, 0)
		}
	}
	return perUserTransfers
}

func GenerateResponse(respStruct interface{}) (events.APIGatewayProxyResponse, error) {
	response, err := json.Marshal(respStruct)
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type":                     "application/json",
			"Access-Control-Allow-Origin":      "*",
			"Access-Control-Allow-Credentials": "true",
			"Access-Control-Allow-Headers":     "Content-Type",
			"Access-Control-Allow-Methods":     "OPTIONS,POST,GET",
		},
		Body: string(response),
	}, err
}

func GetPayerType(transfers []schema.Transfer) schema.PayerType {
	if len(transfers) == 0 {
		return schema.Rentee
	}
	payerType := schema.Rentee
	if transfers[0].ContractAddress == schema.StarSharksRentContractAddresses {
		payerType = schema.Rentee
	} else if transfers[0].ContractAddress == schema.StarSharksPurchaseContractAddresses || transfers[0].ContractAddress == schema.StarSharksAuctionContractAddresses {
		payerType = schema.Purchaser
	}
	for _, transfer := range transfers {
		if payerType == schema.Hybrider {
			continue
		}
		if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
			if payerType == schema.Purchaser {
				payerType = schema.Hybrider
			}
		} else if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {

			if payerType == schema.Rentee {
				payerType = schema.Hybrider
			}
		}
	}
	return payerType
}

func GetPayerTypes(totalTransfers []schema.Transfer) map[string]schema.PayerType {
	userTypes := map[string]schema.PayerType{}

	for _, transfer := range totalTransfers {
		payerAddress := transfer.FromAddress
		if userTypes[payerAddress] == schema.Hybrider {
			continue
		}
		if transfer.ContractAddress == schema.StarSharksRentContractAddresses {
			if userTypes[payerAddress] == schema.Purchaser {
				userTypes[payerAddress] = schema.Hybrider
			} else if userTypes[payerAddress] != schema.Rentee {
				userTypes[payerAddress] = schema.Rentee
			}
		}
		if transfer.ContractAddress == schema.StarSharksPurchaseContractAddresses || transfer.ContractAddress == schema.StarSharksAuctionContractAddresses {
			if userTypes[payerAddress] == schema.Rentee {
				userTypes[payerAddress] = schema.Hybrider
			} else if userTypes[payerAddress] != schema.Purchaser {
				userTypes[payerAddress] = schema.Purchaser
			}
		}
	}
	return userTypes
}

func GetMysteriousBoxTransfers(fromTimeObj time.Time, toTimeObj time.Time, svc *s3.S3) []schema.Transfer {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String(schema.PriceBucketName),
			Key:    aws.String("starsharks-mysterious-box-transfers.csv"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		exitErrorf("Unable to get object, %v", err)
	}
	body, err := ioutil.ReadAll(result.Body)
	if err != nil || body == nil {
		exitErrorf("Unable to get body, %v", err)
	}
	bodyString := string(body)
	lines := strings.Split(bodyString, "\n")
	transfers := make([]schema.Transfer, 0)
	//log.Printf("enterred converCsvStringToTransferStructs, content len: %d", len(lines))
	for lineNum, lineString := range lines {
		if lineNum == 0 {
			continue
		}
		fields := strings.Split(lineString, ",")
		if len(fields) < 8 {
			continue
		}

		layout := "2006-01-02T15:04:05.000Z"
		timeObj, _ := time.Parse(layout, fields[2])
		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
			continue
		}

		transfers = append(transfers, schema.Transfer{
			FromAddress:     fields[0],
			Value:           float64(40 * schema.SeaTokenUnit),
			TransactionHash: fields[1],
			Timestamp:       int(timeObj.Unix()),
		})
	}

	return transfers
}

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}
