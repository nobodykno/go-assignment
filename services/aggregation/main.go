// Command aggregation runs the aggregation stage of the pipeline as a background
// worker. Health and metrics are served on METRICS_PORT.
package main

import "go-assignment/packages/worker"

func main() {
	worker.Main("aggregation", func(deps worker.Deps) worker.Stage {
		aggregation := &AggregationWorker{
			DB:             deps.DB,
			ErrorCollector: deps.Collector,
		}
		return aggregation.Stage()
	})
}
