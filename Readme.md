
# Go Data Processing Pipeline

A concurrent data processing pipeline built with Go that supports multiple data sources, validates and transforms records, performs aggregations, and exports results. A REST API allows users to create jobs and track their processing status.

## Tech Stack

- **Language:** Go
- **HTTP Server:** Go standard library (`net/http`)
- **Database:** PostgreSQL
- **ORM:** GORM
- **Database Migrations:** Goose
- **Concurrency:** Goroutines, channels, and worker pools
- **Testing:** Go testing package
- **Containerization:** Docker and Docker Compose

## Features

- Create and manage data processing jobs through REST APIs.
- Upload CSV and JSON files.
- Support mixed-source processing, combining multiple CSV, JSON, and API sources within a single job.
- Validate records and collect validation errors.
- Transform records according to business rules.
- Aggregate processed data.
- Export processing results.
- Track job status, progress, and record counts.
- Store pipeline errors in PostgreSQL.
- Process records concurrently using goroutines and channels.
- Support context-based cancellation and graceful shutdown.

## Mixed-Source Processing

Mixed-source processing allows a single job to consume data from multiple sources instead of processing only one input file.

A job can contain multiple CSV and JSON files, or API sources configured with their required URLs. Each source has its own name, type, and source configuration.

The pipeline processes the configured sources through the applicable stages, including ingestion, validation, transformation, aggregation, and export.

This approach makes it possible to combine data from different input sources within one processing job while tracking the job's status and errors.

## Project Structure

- `apps/api` — REST API, controllers, routes, middleware, and services.
- `packages/models` — Shared database models.
- `packages/database` — Database connection logic.
- `packages/errors` — Pipeline error collection and persistence.
- `packages/migrations` — Database migrations.
- `services/ingestion` — Data ingestion.
- `services/ingestion-worker` — Background ingestion processing.
- `services/validation` — Record validation and worker pool.
- `services/transformation` — Data transformation.
- `services/aggregation` — Data aggregation.
- `services/export` — Exporting processed results.

## Setup

1. Install Go, PostgreSQL, and Docker if required.
2. Create the development database and configure the database environment variables.
3. Run the database migrations using Goose.
4. Start the API:
   ```
   cd apps/api
   go run .
   ```

## Testing

Configure `TEST_DATABASE_URL` to point to a separate test database before running database-backed integration tests.

```
cd apps/api
go test ./...
```

Keep the test database separate from the development database.

## License

Add the appropriate license information if the project is distributed publicly.

Add API usage examplesDocument environment variablesExplain pipeline flow briefly