package main

import "go-assignment/packages/worker"

func main() {
	worker.Main("transformation", func(deps worker.Deps) worker.Stage {
		transformation := &TransformationWorker{
			DB:             deps.DB,
			ErrorCollector: deps.Collector,
		}
		return transformation.Stage()
	})
}
