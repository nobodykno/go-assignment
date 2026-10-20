package main

import (
	"context"
	"sync"
)

// RecordJob contains a record and its position in the input batch.
type RecordJob struct {
	RecordNumber int    `json:"record_number"`
	Record       Record `json:"record"`
}

// ValidationResult contains the record and its validation errors.
type ValidationResult struct {
    RecordNumber int      `json:"record_number"`
    Record       Record   `json:"record"`
    Errors       []string `json:"errors"`
}

// WorkerPool validates records concurrently.
type WorkerPool struct {
	WorkerCount int
}

// Run starts the worker pool and closes output when all workers finish.
func (p *WorkerPool) Run(
	ctx context.Context,
	input <-chan RecordJob,
	output chan<- ValidationResult,
) {
	workerCount := p.WorkerCount

	if workerCount <= 0 {
		workerCount = 1
	}

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case job, ok := <-input:
					if !ok {
						return
					}

					if ctx.Err() != nil {
						return
					}

					result := ValidateRecordWithNumber(
						job.RecordNumber,
						job.Record,
					)

					select {
					case output <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(output)
}