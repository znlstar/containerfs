package utils

// autoinc struct
type AutoInc struct {
	start, step, Cur int64
	queue            chan int64
	running          bool
}

// new
func New(start, step int64) (ai *AutoInc) {
	ai = &AutoInc{
		start:   start,
		step:    step,
		Cur:     start,
		running: true,
		queue:   make(chan int64, 4),
	}
	go ai.process()
	return
}

func (ai *AutoInc) process() {
	defer func() { recover() }()
	for i := ai.start; ai.running; i = i + ai.step {
		ai.queue <- i
	}
}

// id
func (ai *AutoInc) Id() int64 {
	ai.Cur = <-ai.queue
	return ai.Cur
}

// close
func (ai *AutoInc) Close() {
	ai.running = false
	close(ai.queue)
}
