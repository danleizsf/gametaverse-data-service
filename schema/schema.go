package schema

import (
	"time"
)

type Input struct {
	Method string  `json:"method"`
	Params []Param `json:"params"`
	Path   string  `json:"path"`
}

type GrafanaQueryRequest struct {
	Targets []GrafanaQueryTarget `json:"targets"`
	Range   Range                `json:"range"`
}

type GrafanaQueryTarget struct {
	Target string `json:"target"`
	Type   string `json:"type"`
}

type Range struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Param struct {
	Address       string `json:"address"`
	Timestamp     int64  `json:"timestamp"`
	FromTimestamp int64  `json:"fromTimestamp"`
	ToTimestamp   int64  `json:"toTimestamp"`
	Limit         int64  `json:"limit"`
}

var DailyTransferBucketName = "gametaverse-bucket"
var UserBucketName = "gametaverse-user-bucket"
var PriceBucketName = "gametaverse-price-bucket"
var SeaTokenUnit = 1000000000000000000
var StarSharksGameWalletAddresses = map[string]bool{
	"0x0000000000000000000000000000000000000000": true,
	"0x1f7acc330fe462a9468aa47ecdb543787577e1e7": true,
}
var StarSharksRentContractAddresses = "0xe9e092e46a75d192d9d7d3942f11f116fd2f7ca9"
var StarSharksPurchaseContractAddresses = "0x1f7acc330fe462a9468aa47ecdb543787577e1e7"
var StarSharksAuctionContractAddresses = "0xd78be0b93a3c9d1a9323bca03184accf1a57e548"
var StarSharksWithdrawContractAddresses = "0x94019518f82762bb94280211d19d4ac025d98583"

var StarSharksContractAddresses = map[string]bool{
	"0x0000000000000000000000000000000000000000": true,
	"0x1f7acc330fe462a9468aa47ecdb543787577e1e7": true,
	"0x6bfad6a81d5c7bcd99c071d52d328f9dbbabc4ad": true,
	StarSharksAuctionContractAddresses:           true,
	"0x03961ae0a61f64ac7a68734e97544512ed4233a6": true,
	"0x0e3089db5f6a9f0a6550818d1dc274da9f73d4c8": true,
	StarSharksRentContractAddresses:              true,
	StarSharksPurchaseContractAddresses:          true,
	StarSharksWithdrawContractAddresses:          true,
}

var StarSharksStartingDate = time.Unix(1639612800, 0) // 12-16-2021
var StarSharksJan10 = time.Unix(1641772800, 0)        // 1-10-2022
var StarSharksJan20 = time.Unix(1642636800, 0)        // 1-10-2022

var DayInSec = 86400
var DateFormat = "2006-01-02"

type Dau struct {
	DateTimestamp    int64           `json:"dateTimestamp"`
	TotalActiveUsers ActiveUserCount `json:"totalActiveUsers"`
	NewActiveUsers   ActiveUserCount `json:"newActiveUsers"`
}

type ActiveUserCount struct {
	PayerCount     PayerCount `json:"payerCount"`
	TotalUserCount int64      `json:"totalUserCount"`
}
type PayerCount struct {
	RenteeCount    int64 `json:"renteeCount"`
	PurchaserCount int64 `json:"purchaserCount"`
}
type DailyTransactionVolume struct {
	DateTimestamp          int64                 `json:"dateTimestamp"`
	TotalTransactionVolume UserTransactionVolume `json:"totalTransactionVolume"`
}
type UserTransactionVolume struct {
	TokenVolume                 float64 `json:"tokenVolume"`
	RenterTransactionVolume     int64   `json:"renterTransactionVolume"`
	PurchaserTransactionVolume  int64   `json:"purchaserTransactionVolume"`
	WithdrawerTransactionVolume int64   `json:"withdrawerTransactionVolume"`
}

type UserRoiDetail struct {
	UserAddress        string    `json:"userAddress"`
	JoinDateTimestamp  int64     `json:"joinDateTimestamp,omitempty"`
	TotalSpendingUsd   float64   `json:"totalSpendingUsd"`
	TotalProfitUsd     float64   `json:"totalProfitUsd"`
	TotalGainUsd       float64   `json:"totalGainUsd,omitempty"`
	TotalGainToken     float64   `json:"totalGainToken,omitempty"`
	TotalSpendingToken float64   `json:"totalSpendingToken"`
	TotalProfitToken   float64   `json:"totalProfitToken"`
	ProfitableDays     int64     `json:"profitableDays,omitempty"`
	UserType           PayerType `json:"userType"`
	JoinDate           string    `json:"joinDate,omitempty"`
}

type UserType struct {
	UserAddress string     `json:"userAddress"`
	Type        string     `json:"type"`
	Transfers   []Transfer `json:"transfers"`
}

type UserTypeCount struct {
	RenteeCount    int64 `json:"renteeCount"`
	PurchaserCount int64 `json:"purchaserCount"`
	HybridCount    int64 `json:"hybridCount"`
	OtherCount     int64 `json:"otherCount"`
}

type AllUserRoiDetails struct {
	OverallProfitableRate float64         `json:"overallProfitableRate"`
	UserRoiDetails        []UserRoiDetail `json:"userRoiDetails,omitempty"`
}

type ValueFrequencyPercentage struct {
	Value               int64   `json:"value"`
	FrequencyPercentage float64 `json:"frequencyPercentage"`
}

type UserActivity struct {
	UserAddress      string `json:"userAddress"`
	TotalDatesCount  int64  `json:"totalDatesCount"`
	ActiveDatesCount int64  `json:"activeDatesCount"`
}

type PriceHistory struct {
	ContractAddress string  `json:"contract_address"`
	Prices          []Price `json:"Prices"`
}

type Price struct {
	Date  string  `json:"date"`
	Price float64 `json:"price"`
}

type Transaction struct {
	TransactionHash      string
	Nonce                string
	BlockHash            string
	BlockNumber          int
	TransactionIndex     int
	FromAddress          string
	ToAddress            string
	Value                int
	Gas                  int
	GasPrice             int
	Input                string
	BlockTimestamp       int64
	MaxFeePerGas         int
	MaxPriorityFeePerGas int
}

type Transfer struct {
	TokenAddress    string
	FromAddress     string
	ToAddress       string
	Value           float64
	TransactionHash string
	LogIndex        int
	BlockNumber     int
	Timestamp       int
	ContractAddress string
}

type UserMetaInfo struct {
	Timestamp       int64  `json:"timestamp"`
	TransactionHash string `json:"transaction_hash"`
}

type PayerType int64

const (
	Unknown   PayerType = 0
	Rentee    PayerType = 1
	Purchaser PayerType = 2
	Hybrider  PayerType = 3
)

type WhalesSortType int64

const (
	SortByUnknown  WhalesSortType = 0
	SortByGain     WhalesSortType = 1
	SortByProfit   WhalesSortType = 2
	SortBySpending WhalesSortType = 3
)

// daily aggregate
type Summary struct {
	ActiveUser         []string          `json:"active_user"`
	NewUser            map[string]string `json:"new_user"`
	SeaVolume          float64           `json:"sea_volume"`
	AuctionSharkVolume int64             `json:"auction_shark_volume"`
	CreateSharkVolume  int64             `json:"create_shark_volume"`
	RentSharkVolume    int64             `json:"rent_shark_volume"`
	BuySharkVolume     int64             `json:"buy_shark_volume"`
}

type UserAction struct {
	Date   string      `json:"date"`
	Action string      `json:"action"`
	Value  interface{} `json:"value"`
	Time   time.Time
}

var UserActionRentSharkSEA = "rent_shark_SEA"
var UserActionLendSharkSEA = "lend_rent_SEA"
var UserActionAuctionBuyNFT = "auction_buy_NFT"
var UserActionAuctionSellNFT = "auction_sell_NFT"
var UserActionAuctionBuySEA = "auction_buy_SEA"
var UserActionAuctionSellSEA = "auction_sell_SEA"
var UserActionBuyNFT = "buy_NFT"
var UserActionBuySEA = "buy_SEA"
var UserActionWithdrawlSEA = "withdrawl_SEA"
