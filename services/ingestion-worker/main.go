// Command ingestion-worker reads each pending job's input, sends it to
// the validation service in batches and stores the results. Health and
// metrics are served on METRICS_PORT.
package main

import "go-assignment/packages/worker"

func main() {
	worker.Main("ingestion-worker", func(deps worker.Deps) worker.Stage {
		ingestion := &JobWorker{
			DB:             deps.DB,
			ErrorCollector: deps.Collector,
		}
		return ingestion.Stage()
	})
}
