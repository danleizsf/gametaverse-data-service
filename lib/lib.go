package lib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gametaverse-data-service/schema"
	"io/ioutil"
	"log"
	"os"
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
		return nil
	}
	body = bytes.Replace(body, []byte(":NaN"), []byte(":null"), -1)
	err = json.Unmarshal(body, &s)
	if err != nil {
		log.Print("can't unmarshall object " + *req.Key)
		return nil
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

func GetPerPayerType(ua []schema.UserAction) schema.PayerType {
	totalRentingValue := float64(0)
	totalInvestingValue := float64(0)
	for _, a := range ua {
		if a.Action == schema.UserActionAuctionBuySEA || a.Action == schema.UserActionBuySEA {
			totalInvestingValue += a.Value.(float64)
		} else if a.Action == schema.UserActionRentSharkSEA {
			totalRentingValue += a.Value.(float64)
		}
	}
	if totalInvestingValue > totalRentingValue {
		return schema.Purchaser
	} else {
		return schema.Rentee
	}
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

func ExitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}

func GetPriceHistoryV2(svc *s3.S3) schema.PriceHistory {
	requestInput :=
		&s3.GetObjectInput{
			Bucket: aws.String("gametaverse-daily-starsharks"),
			Key:    aws.String("sea-token-price-history.json"),
		}
	result, err := svc.GetObject(requestInput)
	if err != nil {
		ExitErrorf("Unable to get sea-token-price-history.json, %v", err)
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

// func GetMysteriousBoxTransfers(fromTimeObj time.Time, toTimeObj time.Time, svc *s3.S3) []schema.Transfer {
// 	requestInput :=
// 		&s3.GetObjectInput{
// 			Bucket: aws.String(schema.PriceBucketName),
// 			Key:    aws.String("starsharks-mysterious-box-transfers.csv"),
// 		}
// 	result, err := svc.GetObject(requestInput)
// 	if err != nil {
// 		exitErrorf("Unable to get object, %v", err)
// 	}
// 	body, err := ioutil.ReadAll(result.Body)
// 	if err != nil || body == nil {
// 		exitErrorf("Unable to get body, %v", err)
// 	}
// 	bodyString := string(body)
// 	lines := strings.Split(bodyString, "\n")
// 	transfers := make([]schema.Transfer, 0)
// 	for lineNum, lineString := range lines {
// 		if lineNum == 0 {
// 			continue
// 		}
// 		fields := strings.Split(lineString, ",")
// 		if len(fields) < 8 {
// 			continue
// 		}

// 		layout := "2006-01-02T15:04:05.000Z"
// 		timeObj, _ := time.Parse(layout, fields[2])
// 		if timeObj.Before(fromTimeObj) || timeObj.After(toTimeObj) {
// 			continue
// 		}

// 		transfers = append(transfers, schema.Transfer{
// 			FromAddress:     fields[0],
// 			Value:           float64(40 * schema.SeaTokenUnit),
// 			TransactionHash: fields[1],
// 			Timestamp:       int(timeObj.Unix()),
// 		})
// 	}

// 	return transfers
// }

func exitErrorf(msg string, args ...interface{}) {
	log.Printf(msg + "\n")
	os.Exit(1)
}
