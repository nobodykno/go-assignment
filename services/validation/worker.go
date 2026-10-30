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
				// Stop before receiving another record if cancelled.
				if ctx.Err() != nil {
					return
				}

				select {
				case <-ctx.Done():
					return

				case job, ok := <-input:
					if !ok {
						return
					}

					// Avoid processing a record after cancellation.
					if ctx.Err() != nil {
						return
					}

					result := ValidateRecordWithNumber(
						job.RecordNumber,
						job.Record,
					)

					// Avoid publishing the result if cancellation
					// occurred while the record was being validated.
					if ctx.Err() != nil {
						return
					}

					select {
					case output <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Wait for every worker before closing output.
	// Only this function closes the output channel.
	wg.Wait()
	close(output)
}
