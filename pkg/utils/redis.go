package utils

import (
	"fmt"
	"strconv"
)

const ProcessedPaymentsPrefix = "processed:payments"

func BuildTimelineKey(processor string) string {
	return fmt.Sprintf("%s:%s:timeline", ProcessedPaymentsPrefix, processor)
}

func BuildStatsKey(processor string) string {
	return fmt.Sprintf("%s:%s:stats", ProcessedPaymentsPrefix, processor)
}

func BuildSortedSetMember(amount float64, correlationID string) string {
	return strconv.FormatFloat(amount, 'f', 2, 64) + ":" + correlationID
}