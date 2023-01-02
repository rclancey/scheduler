package scheduler

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Schedule interface {
	Next(t time.Time) time.Time
}

type Callable interface {
	Call()
}

type CallableFunction func()

func (cf CallableFunction) Call() {
	cf()
}

type Job interface {
	ID() string
	SetName(string)
	PrepareNext(t time.Time) bool
	Next() time.Time
	Do(callback Callable)
	DoOnce(callback Callable)
	AndAfterExactly(delay time.Duration) Job
	AndAfterAround(delay, stddev time.Duration) Job
	AndAfterAtLeast(delay, stddev time.Duration) Job
	AndAfterAtMost(delay, stddev time.Duration) Job
	Run(t time.Time) []Job
	Cancel()
	Cancelled() bool
	Ready(t time.Time) bool
	Export() *JobConfig
}

type withFollowups interface {
	setFollowups(jobs []Job)
}

type BaseJob struct {
	mutex *sync.Mutex
	id uuid.UUID
	name string
	scheduler *Scheduler
	next time.Time
	callback Callable
	followups []Job
	cancelled bool
	runCount int
	once bool
	last *time.Time
}

func NewBaseJob(scheduler *Scheduler) *BaseJob {
	id, _ := uuid.NewRandom()
	return &BaseJob{
		mutex: &sync.Mutex{},
		id: id,
		scheduler: scheduler,
	}
}

func (job *BaseJob) Export() *JobConfig {
	if job.Cancelled() {
		return nil
	}
	cfg := &JobConfig{
		ID: job.ID(),
		Name: job.name,
		Webhook: job.callback.(*Webhook),
		Followups: make([]*JobConfig, 0, len(job.followups)),
		Last: job.last,
	}
	nt := job.next
	if !nt.IsZero() {
		cfg.Next = &nt
	}
	for _, xj := range job.followups {
		xcfg := xj.Export()
		if xcfg != nil {
			cfg.Followups = append(cfg.Followups, xcfg)
		}
	}
	return cfg
}

func (job *BaseJob) setFollowups(jobs []Job) {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	job.followups = jobs
}

func (job *BaseJob) ID() string {
	return job.id.String()
}

func (job *BaseJob) SetName(name string) {
	job.name = name
}

func (job *BaseJob) PrepareNext(t time.Time) bool {
	job.next = time.Time{}
	return false
}

func (job *BaseJob) Next() time.Time {
	return job.next
}

func (job *BaseJob) Ready(t time.Time) bool {
	if job.Cancelled() {
		log.Printf("job %s cancelled", job.ID())
		return false
	}
	nt := job.Next()
	if nt.IsZero() {
		log.Printf("job %s not scheduled", job.ID())
		return false
	}
	if nt.After(t) {
		log.Printf("job %s not ready (%s)", job.ID(), nt)
		return false
	}
	log.Printf("job %s ready (%s)", job.ID(), nt)
	return true
}

func (job *BaseJob) Do(callback Callable) {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	job.callback = callback
	job.once = false
}

func (job *BaseJob) DoOnce(callback Callable) {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	job.callback = callback
	job.once = true
	job.runCount = 0
}

func (job *BaseJob) AndAfterExactly(delay time.Duration) Job {
	return job.AndAfterAround(delay, 0)
}

func (job *BaseJob) AndAfterAround(delay, stddev time.Duration) Job {
	xj := NewDelayedJob(job.scheduler, delay, stddev, delay - 4 * stddev, delay + 4 * stddev)
	job.mutex.Lock()
	job.followups = append(job.followups, xj)
	job.mutex.Unlock()
	return xj
}

func (job *BaseJob) AndAfterAtLeast(delay, stddev time.Duration) Job {
	xj := NewDelayedJob(job.scheduler, delay, stddev, delay, delay + 4 * stddev)
	job.mutex.Lock()
	job.followups = append(job.followups, xj)
	job.mutex.Unlock()
	return xj
}

func (job *BaseJob) AndAfterAtMost(delay, stddev time.Duration) Job {
	xj := NewDelayedJob(job.scheduler, delay, stddev, delay - 4 * stddev, delay)
	job.mutex.Lock()
	job.followups = append(job.followups, xj)
	job.mutex.Unlock()
	return xj
}

func (job *BaseJob) Run(t time.Time) []Job {
	if !job.Ready(t) {
		return nil
	}
	job.mutex.Lock()
	defer job.mutex.Unlock()
	jobs := []Job{}
	job.runCount++
	job.last = &t
	if job.callback != nil {
		log.Printf("running callback for %s", job.ID())
		job.callback.Call()
	} else {
		log.Printf("no callback for %s", job.ID())
	}
	job.next = time.Time{}
	for _, xj := range job.followups {
		xj.PrepareNext(t)
		if !xj.Next().IsZero() {
			jobs = append(jobs, xj)
		}
	}
	return jobs
}

func (job *BaseJob) Cancel() {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	job.cancelled = true
	job.next = time.Time{}
	for _, xj := range job.followups {
		xj.Cancel()
	}
}

func (job *BaseJob) Cancelled() bool {
	job.mutex.Lock()
	defer job.mutex.Unlock()
	return job.cancelled || (job.once && job.runCount > 0)
}

type DelayedJob struct {
	*BaseJob
	delay time.Duration
	stddev time.Duration
	minDelay time.Duration
	maxDelay time.Duration
	seed int64
}

func NewDelayedJob(scheduler *Scheduler, delay, stddev, min, max time.Duration) *DelayedJob {
	parent := NewBaseJob(scheduler)
	r := bytes.NewReader(parent.id[:])
	var seed int64
	binary.Read(r, binary.BigEndian, &seed)
	return &DelayedJob{
		BaseJob: parent,
		delay: delay,
		stddev: stddev,
		minDelay: min,
		maxDelay: max,
		seed: seed,
	}
}

func (job *DelayedJob) Export() *JobConfig {
	cfg := job.BaseJob.Export()
	if cfg == nil {
		return nil
	}
	delay := job.delay.Seconds()
	if job.stddev > 0 {
		stddev := job.stddev.Seconds()
		cfg.Stddev = &stddev
		if job.minDelay == job.delay {
			cfg.MinDelay = &delay
		} else if job.maxDelay == job.delay {
			cfg.MaxDelay = &delay
		} else {
			cfg.Delay = &delay
		}
		cfg.Seed = &job.seed
	} else {
		cfg.Delay = &delay
	}
	return cfg
}

func (job *DelayedJob) PrepareNext(t time.Time) bool {
	job.BaseJob.mutex.Lock()
	defer job.BaseJob.mutex.Unlock()
	if job.BaseJob.cancelled {
		return false
	}
	delay := getRandomDelay(t, job.delay, job.stddev, job.seed)
	if delay < job.minDelay {
		delay = job.minDelay
	}
	if delay > job.maxDelay {
		delay = job.maxDelay
	}
	job.BaseJob.next = t.Add(delay)
	return true
}

type ScheduledJob struct {
	*BaseJob
	schedule Schedule
}

func NewScheduledJob(scheduler *Scheduler, schedule Schedule) *ScheduledJob {
	return &ScheduledJob{
		BaseJob: NewBaseJob(scheduler),
		schedule: schedule,
	}
}

func (job *ScheduledJob) Export() *JobConfig {
	cfg := job.BaseJob.Export()
	if cfg == nil {
		return nil
	}
	switch sched := job.schedule.(type) {
	case *RandomizedCrontab:
		ct := sched.Crontab.String()
		cfg.Crontab = &ct
		cfg.Stddev = &sched.StddevSec
		cfg.Seed = &sched.Seed
	case *Crontab:
		ct := sched.String()
		cfg.Crontab = &ct
	}
	return cfg
}

func (job *ScheduledJob) PrepareNext(t time.Time) bool {
	if job.Cancelled() {
		return false
	}
	job.BaseJob.next = job.schedule.Next(t)
	return true
}

func (job *ScheduledJob) Run(t time.Time) []Job {
	jobs := job.BaseJob.Run(t)
	if job.PrepareNext(t) {
		jobs = append(jobs, job)
	}
	return jobs
}

type Scheduler struct {
	jobs map[string]Job
	timer *Timer
	mutex *sync.Mutex
	wg *sync.WaitGroup
	running bool
}

func NewScheduler(jobs []Job) *Scheduler {
	s := &Scheduler{
		jobs: map[string]Job{},
		mutex: &sync.Mutex{},
		wg: &sync.WaitGroup{},
	}
	for _, job := range jobs {
		s.jobs[job.ID()] = job
	}
	return s
}

func (s *Scheduler) At(t time.Time) Job {
	job := NewBaseJob(s)
	job.next = t
	job.once = true
	s.Add(job)
	return job
}

func (s *Scheduler) Regularly(sched Schedule) Job {
	job := NewScheduledJob(s, sched)
	job.next = sched.Next(time.Now())
	s.Add(job)
	return job
}

func (s *Scheduler) doCallbacks(t time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	newJobs := []Job{}
	for _, job := range s.jobs {
		if job.Ready(t) {
			newJobs = append(newJobs, job.Run(t)...)
		}
	}
	for _, job := range newJobs {
		s.jobs[job.ID()] = job
	}
}

func (s *Scheduler) prepareTimer() *Timer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var t time.Time
	toDelete := []string{}
	for id, job := range s.jobs {
		if job.Cancelled() {
			toDelete = append(toDelete, id)
		} else {
			jt := job.Next()
			if jt.IsZero() {
				toDelete = append(toDelete, id)
			} else if t.IsZero() || jt.Before(t) {
				t = jt
			}
		}
	}
	for _, id := range toDelete {
		delete(s.jobs, id)
	}
	if t.IsZero() {
		log.Println("no jobs, sleeping for an hour")
		s.timer = NewTimer(time.Hour)
	} else {
		log.Println("sleeping until", t)
		s.timer = NewTimer(time.Until(t))
	}
	return s.timer
}

func (s *Scheduler) runOnce() bool {
	timer := s.prepareTimer()
	t, ok := timer.Wait()
	if !ok {
		log.Println("timer cancelled")
		return false
	}
	log.Println("running callbacks for", t)
	s.doCallbacks(t)
	return true
}

func (s *Scheduler) Run() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.subRun()
}

func (s *Scheduler) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.subStop() {
		s.subRun()
	}
}

func (s *Scheduler) subRun() {
	if s.running {
		return
	}
	s.running = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			if !s.runOnce() {
				s.mutex.Lock()
				s.running = false
				s.mutex.Unlock()
				return
			}
		}
	}()
}

func (s *Scheduler) Stop() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.subStop()
}

func (s *Scheduler) subStop() bool {
	running := s.running
	if s.timer != nil {
		s.timer.Stop()
	}
	s.wg.Wait()
	return running
}

func (s *Scheduler) Get(id uuid.UUID) Job {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.jobs[id.String()]
}

func (s *Scheduler) Add(job Job) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.subAdd(job)
}

func (s *Scheduler) subAdd(job Job) {
	running := s.subStop()
	s.jobs[job.ID()] = job
	if running {
		s.subRun()
	}
}

func (s *Scheduler) Remove(job Job) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	running := s.subStop()
	delete(s.jobs, job.ID())
	if running {
		s.subRun()
	}
}

func (s *Scheduler) Import(cfgs []*JobConfig) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	running := s.subStop()
	s.importSub(cfgs)
	if running {
		s.subRun()
	}
}

func (s *Scheduler) importSub(cfgs []*JobConfig) []Job {
	jobs := []Job{}
	for _, cfg := range cfgs {
		job, err := s.importOne(cfg)
		if err == nil {
			jobs = append(jobs, job)
			xjob, ok := job.(withFollowups)
			if ok {
				xjob.setFollowups(s.importSub(cfg.Followups))
			}
			s.jobs[job.ID()] = job
			log.Println("imported job", job.ID())
		} else {
			log.Println("error importing job", err)
		}
	}
	return jobs
}

func (s *Scheduler) importOne(cfg *JobConfig) (Job, error) {
	base := NewBaseJob(s)
	id, err := uuid.Parse(cfg.ID)
	if err == nil {
		base.id = id
	}
	base.name = cfg.Name
	base.last = cfg.Last
	if cfg.Once {
		base.DoOnce(cfg.Webhook)
	} else {
		base.Do(cfg.Webhook)
	}
	if cfg.Crontab != nil {
		var sched Schedule
		var err error
		if cfg.Stddev != nil {
			var seed int64
			if cfg.Seed != nil {
				seed = *cfg.Seed
			}
			sched, err = NewRandomizedCrontab(*cfg.Crontab, *cfg.Stddev, seed)
		} else {
			sched, err = NewCrontab(*cfg.Crontab)
		}
		if err != nil {
			return nil, err
		}
		job := &ScheduledJob{base, sched}
		job.PrepareNext(time.Now())
		return job, nil
	} else if cfg.Delay != nil || cfg.MinDelay != nil || cfg.MaxDelay != nil {
		var seed int64
		if cfg.Seed != nil {
			seed = *cfg.Seed
		}
		return &DelayedJob{
			BaseJob: base,
			delay: cfg.DelayDuration(),
			stddev: cfg.StddevDuration(),
			minDelay: cfg.MinDelayDuration(),
			maxDelay: cfg.MaxDelayDuration(),
			seed: seed,
		}, nil
	}
	return base, nil
}

func (s *Scheduler) Export() []*JobConfig {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	all := []*JobConfig{}
	for _, job := range s.jobs {
		all = append(all, job.Export())
	}
	seen := map[string]bool{}
	for _, cfg := range all {
		for _, xcfg := range cfg.Followups {
			seen[xcfg.ID] = true
		}
	}
	top := []*JobConfig{}
	for _, cfg := range all {
		if !seen[cfg.ID] {
			top = append(top, cfg)
		}
	}
	return top
}
