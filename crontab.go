package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCrontab = errors.New("invalid crontab")

type Crontab struct {
	parts    []string
	months   map[int]bool
	weekdays map[int]bool
	days     map[int]bool
	hours    map[int]bool
	minutes  map[int]bool
}

func parseCrontabPart(part string, min, max int) (map[int]bool, error) {
	parts := strings.Split(part, ",")
	vals := map[int]bool{}
	for _, v := range parts {
		if v == "*" {
			for i := min; i <= max; i++ {
				vals[i] = true
			}
		} else if strings.HasPrefix(v, "*/") {
			d, err := strconv.Atoi(v[2:])
			if err != nil {
				return nil, err
			}
			if d == 0 {
				return nil, errors.New("division by zero")
			}
			for i := min; i <= max; i += d {
				vals[i] = true
			}
		} else if strings.Contains(v, "-") {
			r := strings.Split(v, "-")
			if len(r) > 2 {
				return nil, errors.New("invalid range")
			}
			start, err := strconv.Atoi(r[0])
			if err != nil {
				return nil, err
			}
			if start < min {
				start = min
			}
			end, err := strconv.Atoi(r[1])
			if err != nil {
				return nil, err
			}
			if err != nil {
				return nil, err
			}
			if end > max {
				end = max
			}
			for i := start; i <= end; i++ {
				vals[i] = true
			}
		} else {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			vals[n] = true
		}
	}
	return vals, nil
}

func NewCrontab(crontab string) (*Crontab, error) {
	parts := strings.Fields(strings.TrimSpace(crontab))
	if len(parts) < 5 {
		return nil, fmt.Errorf("%w: missing %d fields", ErrInvalidCrontab, 5 - len(parts))
	}
	if len(parts) > 5 {
		return nil, fmt.Errorf("%w: %d extra fields", ErrInvalidCrontab, len(parts) - 5)
	}
	ct := &Crontab{parts: parts}
	err := ct.SetMinutes(parts[0])
	if err != nil  {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCrontab, err)
	}
	err = ct.SetHours(parts[1])
	if err != nil  {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCrontab, err)
	}
	err = ct.SetDays(parts[2])
	if err != nil  {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCrontab, err)
	}
	err = ct.SetMonths(parts[3])
	if err != nil  {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCrontab, err)
	}
	err = ct.SetWeekdays(parts[4])
	if err != nil  {
		return nil, fmt.Errorf("%w: %s", ErrInvalidCrontab, err)
	}
	return ct, nil
}

func (ct *Crontab) String() string {
	return strings.Join(ct.parts, " ")
}

func (ct *Crontab) MarshalJSON() ([]byte, error) {
	return json.Marshal(strings.Join(ct.parts, " "))
}

func (ct *Crontab) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	xct, err := NewCrontab(str)
	if err != nil {
		return err
	}
	*ct = *xct
	return nil
}

func (ct *Crontab) SetMinutes(str string) error {
	minutes, err := parseCrontabPart(str, 0, 59)
	if err != nil {
		return err
	}
	ct.parts[0] = str
	ct.minutes = minutes
	return nil
}

func (ct *Crontab) SetHours(str string) error {
	hours, err := parseCrontabPart(str, 0, 23)
	if err != nil {
		return err
	}
	ct.parts[1] = str
	ct.hours = hours
	return nil
}

func (ct *Crontab) SetDays(str string) error {
	days, err := parseCrontabPart(str, 1, 31)
	if err != nil {
		return err
	}
	ct.parts[2] = str
	ct.days = days
	return nil
}

func (ct *Crontab) SetMonths(str string) error {
	months, err := parseCrontabPart(str, 1, 12)
	if err != nil {
		return err
	}
	ct.parts[3] = str
	ct.months = months
	return nil
}

func (ct *Crontab) SetWeekdays(str string) error {
	weekdays, err := parseCrontabPart(str, 0, 6)
	if err != nil {
		return err
	}
	ct.parts[4] = str
	ct.weekdays = weekdays
	return nil
}

func (ct *Crontab) MatchMonth(t time.Time) bool {
	return ct.months[int(t.Month())]
}

func (ct *Crontab) MatchDate(t time.Time) bool {
	if !ct.MatchMonth(t) {
		return false
	}
	return ct.days[t.Day()] && ct.weekdays[int(t.Weekday())]
}

func  (ct *Crontab) MatchHour(t time.Time) bool {
	if !ct.MatchDate(t) {
		return false
	}
	return ct.hours[t.Hour()]
}

func (ct *Crontab) MatchTime(t time.Time) bool {
	if !ct.MatchHour(t) {
		return false
	}
	return ct.minutes[t.Minute()]
}

func (ct *Crontab) Between(start, end time.Time) []time.Time {
	minutes := []time.Time{}
	for hour := 0; hour < 24; hour++ {
		if !ct.hours[hour] {
			continue
		}
		for minute := 0; minute < 60; minute++ {
			if !ct.minutes[minute] {
				continue
			}
			minutes = append(minutes, time.Date(1970, time.January, 1, hour, minute, 0, 0, time.UTC))
		}
	}
	times := []time.Time{}
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		if !ct.MatchDate(d) {
			continue
		}
		for _, minute := range minutes {
			t := time.Date(d.Year(), d.Month(), d.Day(), minute.Hour(), minute.Minute(), 0, 0, d.Location())
			if t.Before(start) || !t.Before(end) {
				continue
			}
			times = append(times, t)
		}
	}
	return times
}

func (ct *Crontab) Next(t time.Time) time.Time {
	start := t
	for {
		if ct.MatchDate(start) {
			for hour := start.Hour(); hour < 24; hour++ {
				if ct.hours[hour] {
					for minute := 0; minute < 60; minute++ {
						if !ct.minutes[minute] {
							continue
						}
						xt := time.Date(t.Year(), t.Month(), t.Day(), hour, minute, 0, 0, t.Location())
						if xt.After(t) {
							return xt
						}
					}
				}
			}
		}
		start = t.AddDate(0, 0, 1)
		start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
		if !ct.MatchMonth(start) {
			start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, start.Location()).AddDate(0, 1, 0)
		}
	}
}

type RandomizedCrontab struct {
	*Crontab
	StddevSec float64
	Seed int64
}

func NewRandomizedCrontab(crontab string, stddevSec float64, seed int64) (*RandomizedCrontab, error) {
	ct, err := NewCrontab(crontab)
	if err != nil {
		return nil, err
	}
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	return &RandomizedCrontab{
		Crontab: ct,
		StddevSec: stddevSec,
		Seed: seed,
	}, nil
}

func (rct *RandomizedCrontab) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"crontab": rct.Crontab,
		"stddev_sec": rct.StddevSec,
		"seed": rct.Seed,
	})
}

func (rct *RandomizedCrontab) UnmarshalJSON(data []byte) error {
	m := map[string]interface{}{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	xrct, err := NewRandomizedCrontab(m["crontab"].(string), m["stddev_sec"].(float64), m["seed"].(int64))
	if err != nil {
		return err
	}
	*rct = *xrct
	return nil
}

func (rct *RandomizedCrontab) randomize(t, min time.Time) time.Time {
	dur := getRandomDelay(t, 0, time.Duration(rct.StddevSec * float64(time.Second)), rct.Seed)
	xt := t.Add(dur)
	if xt.Before(min) {
		return min
	}
	return xt
}

func (rct *RandomizedCrontab) Between(start, end time.Time) []time.Time {
	times := rct.Crontab.Between(start, end)
	out := make([]time.Time, 0, len(times))
	for i, t := range times {
		times[i] = rct.randomize(t, start)
		if i == 0 || times[i].Sub(times[i-1]) > 30 * time.Second {
			out = append(out, times[i])
		}
	}
	return times
}

func (rct *RandomizedCrontab) Next(t time.Time) time.Time {
	xt := rct.randomize(rct.Crontab.Next(t), t)
	add := time.Duration(0)
	for xt.Sub(t) < 30 * time.Second {
		add += time.Minute
		xt = rct.randomize(rct.Crontab.Next(t.Add(add)), t.Add(30*time.Second))
	}
	return xt
}
