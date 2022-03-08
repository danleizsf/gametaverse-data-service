package daily

// func GetUserRetentionRate(s3client *s3.S3, timestampA int64, timestampB int64) float64 {
// 	dateA := lib.GetDate(timestampA)
// 	dateB := lib.GetDate(timestampB)
// 	log.Print(dateA, dateB)

// 	sA := lib.GetSummary(s3client, dateA)
// 	sB := lib.GetSummary(s3client, dateB)

// 	timeActiveUsers := sA.ActiveUser
// 	timeActiveUsersMap := make(map[string]bool)
// 	for i := 0; i < len(timeActiveUsers); i++ {
// 		timeActiveUsersMap[timeActiveUsers[i]] = true
// 	}

// 	timeBctiveUsers := sB.ActiveUser
// 	retentionedUsers := map[string]bool{}
// 	for _, timeBctiveUser := range timeBctiveUsers {
// 		if _, ok := timeActiveUsersMap[timeBctiveUser]; ok {
// 			retentionedUsers[timeBctiveUser] = true
// 		}
// 	}
// 	return float64(len(retentionedUsers)) / float64(len(timeActiveUsers))
// }
