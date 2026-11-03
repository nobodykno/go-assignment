// Command export runs the export stage of the pipeline as a background
// worker. Health and metrics are served on METRICS_PORT.
package main

import "go-assignment/packages/worker"

func main() {
	worker.Main("export", func(deps worker.Deps) worker.Stage {
		export := &ExportWorker{
			DB:             deps.DB,
			ErrorCollector: deps.Collector,
		}
		return export.Stage()
	})
}
