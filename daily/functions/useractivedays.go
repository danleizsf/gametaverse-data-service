package daily

import (
	"container/heap"
	"gametaverse-data-service/lib"
	"gametaverse-data-service/schema"
	"math"

	"github.com/aws/aws-sdk-go/service/s3"
)

type UserActivityItem struct {
	UserActivity schema.UserActivity
	priority     int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.

}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*UserActivityItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*UserActivityItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *UserActivityItem, value schema.UserActivity, priority int) {
	item.UserActivity = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

func GetUserActiveDays(s3client *s3.S3, timestampA int64, timestampB int64, limit int64) []schema.UserActivity {
	useractions := lib.GetUserActionsRange(s3client, timestampA, timestampB)
	pq := make(PriorityQueue, len(useractions))
	i := 0
	for userAddress, actions := range useractions {
		firstDate := actions[0].Time
		lastDate := actions[len(actions)-1].Time
		activeDays := map[string]bool{}
		for _, a := range actions {
			activeDays[a.Date] = true
		}
		ua := schema.UserActivity{
			UserAddress:      userAddress,
			TotalDatesCount:  int64(lastDate.Sub(firstDate).Hours()) / 24,
			ActiveDatesCount: int64(len(activeDays)),
		}
		pq[i] = &UserActivityItem{
			UserActivity: ua,
			index:        i,
			priority:     int(ua.TotalDatesCount),
		}
		i++
	}
	// sort.Slice(perUserActivities, func(i, j int) bool {
	// 	return perUserActivities[i].TotalDatesCount > perUserActivities[j].TotalDatesCount
	// })
	heap.Init(&pq)
	resLimit := int(math.Min(float64(limit), float64(len(useractions))))
	perUserActivities := make([]schema.UserActivity, limit)
	for i := 0; i < resLimit; i++ {
		perUserActivities[i] = heap.Pop(&pq).(*UserActivityItem).UserActivity
	}
	return perUserActivities
}
