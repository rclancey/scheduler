package scheduler

import (
	"math/rand"
	"time"
)

func seedFromTime(t time.Time) int64 {
	seed := int64(t.Second())
	seed += 60 * int64(t.Minute())
	seed += 60 * 60 * int64(t.Hour())
	seed += 60 * 60 * 24 * int64(t.YearDay())
	seed += 60 * 60 * 24 * 366 * int64(t.Year())
	return seed
}

func getRandomDelay(t time.Time, dur, stddev time.Duration, seed int64) time.Duration {
	if stddev == 0 {
		return dur
	}
	seed += seedFromTime(t)
	return time.Duration(rand.New(rand.NewSource(seed)).NormFloat64() * float64(stddev))
}
