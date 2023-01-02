package scheduler

import (
	"time"
)

type JobConfig struct {
	ID        string       `json:"id"`
	Name      string       `json:"name,omitempty"`
	Once      bool         `json:"once,omitempty"`
	Crontab   *string      `json:"crontab,omitempty"`
	Delay     *float64     `json:"delay,omitempty"`
	MinDelay  *float64     `json:"min_delay,omitempty"`
	MaxDelay  *float64     `json:"max_delay,omitempty"`
	Stddev    *float64     `json:"stddev,omitempty"`
	Seed      *int64       `json:"seed,omitempty"`
	Webhook   *Webhook     `json:"webhook,omitempty"`
	Followups []*JobConfig `json:"followups,omitempty"`
	Last      *time.Time   `json:"last,omitempty"`
	Next      *time.Time   `json:"next,omitempty"`
}

func floatPtrToDuration(p *float64) time.Duration {
	if p == nil {
		return 0
	}
	return time.Duration(*p * float64(time.Second))
}

func (cfg *JobConfig) DelayDuration() time.Duration {
	if cfg.Delay != nil {
		return floatPtrToDuration(cfg.Delay)
	}
	if cfg.MinDelay != nil {
		return floatPtrToDuration(cfg.MinDelay)
	}
	if cfg.MaxDelay != nil {
		return floatPtrToDuration(cfg.MaxDelay)
	}
	return 0
}

func (cfg *JobConfig) MinDelayDuration() time.Duration {
	if cfg.MinDelay != nil {
		return floatPtrToDuration(cfg.MinDelay)
	}
	if cfg.Delay != nil {
		return floatPtrToDuration(cfg.Delay) - 4 * cfg.StddevDuration()
	}
	return 0
}

func (cfg *JobConfig) MaxDelayDuration() time.Duration {
	if cfg.MaxDelay != nil {
		return floatPtrToDuration(cfg.MaxDelay)
	}
	if cfg.Delay != nil {
		return floatPtrToDuration(cfg.Delay) + 4 * cfg.StddevDuration()
	}
	return 0
}

func (cfg *JobConfig) StddevDuration() time.Duration {
	return floatPtrToDuration(cfg.Stddev)
}


type ScheduleConfig []*JobConfig
