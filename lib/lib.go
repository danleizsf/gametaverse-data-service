package lib

import (
	"bytes"
	"encoding/json"
	"gametaverse-data-service/schema"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	DailyBucketName = "gametaverse-daily"
)

func GetDate(timestamp int64) string {
	t := time.Unix(timestamp, 0).UTC() //UTC returns t with the location set to UTC.
	return t.Format("2006-01-02")
}

func GetSummary(s3client *s3.S3, date string) schema.Summary {
	var s schema.Summary
	summaryRequest := &s3.GetObjectInput{
		Bucket: aws.String(DailyBucketName),
		Key:    aws.String("starsharks/" + date + "/summary.json"),
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
		log.Print("can't unmarshall object " + *req.Key)
	}
	return s
}

func GetUserActionsRange(s3client *s3.S3, timestampA int64, timestampB int64) map[string][]schema.UserAction {
	start := time.Unix(timestampA, 0)
	end := time.Unix(timestampB, 0)
	length := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		length++
	}
	useractionsByDate := make([]map[string][]schema.UserAction, length+1)
	var wg sync.WaitGroup
	wg.Add(length)
	i := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		go func(s3client *s3.S3, d time.Time) {
			defer wg.Done()
			date := d.Format(schema.DateFormat)
			ua := GetUserActions(s3client, date)
			useractionsByDate[i] = ua
		}(s3client, d)
		i++
	}
	wg.Wait()
	j := 0
	useractions := make(map[string][]schema.UserAction, 0)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		ua := useractionsByDate[j]
		date := d.Format(schema.DateFormat)
		for user, actions := range ua {
			actionsWithDate := make([]schema.UserAction, len(actions))
			for i, a := range actions {
				actionsWithDate[i] = schema.UserAction{
					Value:  a.Value,
					Date:   date,
					Action: a.Action,
				}
			}

			if existingActions, exists := useractions[user]; exists {
				useractions[user] = append(existingActions, actionsWithDate...)
			} else {
				useractions[user] = actionsWithDate
			}
		}
		j++
	}

	return useractions
}

func GetUserActions(s3client *s3.S3, date string) map[string][]schema.UserAction {
	var s map[string][]schema.UserAction
	req := &s3.GetObjectInput{
		Bucket: aws.String(DailyBucketName),
		Key:    aws.String("starsharks/" + date + "/user_actions.json"),
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
			Bucket: aws.String("gametaverse-daily"),
			Key:    aws.String("starsharks/sea-token-price-history.json"),
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
		//log.Printf("body: %s", fmt.Sprintf("%s", body))
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
