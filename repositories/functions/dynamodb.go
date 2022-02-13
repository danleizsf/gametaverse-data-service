package repo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	// "github.com/aws/aws-sdk-go-v2/service/dynamodb/expression"
)

type transfer struct {
	BlockNumber int32
	LogIndex    int32
	Info        map[string]string
}

var seaTokenUnit = 1000000000000000000

func GetBlockTransfer(blockNumber int) string {
	// sess := session.Must(session.NewSession())
	// svc := dynamodb.New(sess, aws.NewConfig().WithEndpoint("http://localhost:8000"))

	// tables, _ := svc.ListTables(&dynamodb.ListTablesInput{})
	// return fmt.Sprintf("%+v", tables.TableNames)
	dynamodbClient := CreateLocalClient()
	// expression.Name("BlockNumber").Equal(expression.Value(14852202))
	output, _ := dynamodbClient.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              aws.String("gametaverse-starsharks-token-transfers"),
		KeyConditionExpression: aws.String("BlockNumber = :blocknumber"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":blocknumber": &types.AttributeValueMemberN{Value: string(blockNumber)},
		},
	})

	// output, _ := dynamodbClient.Scan(context.Background(), &dynamodb.ScanInput{
	// 	TableName: aws.String("gametaverse-starsharks-token-transfers"),
	// 	Limit:     aws.Int32(100),
	// })
	resp := ""
	for _, item := range output.Items {
		var t transfer
		attributevalue.UnmarshalMap(item, &t)
		resp = resp + fmt.Sprintf("\n%+v", t)
		// resp = resp + fmt.Sprint("\n%+v", item["info"].Value)
	}
	return resp
}

func CreateLocalClient() *dynamodb.Client {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID && region == "us-west-1" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           "http://localhost:8000",
				SigningRegion: "us-west-1",
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func main() {
	fmt.Print(GetBlockTransfer(14852202))
}
