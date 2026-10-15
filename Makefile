.PHONY: run agent-sim inject-critical spike test build-go docker-up pages

# Start the central ingestion pipeline & React Analytics Dashboard on http://localhost:8080
run:
	@chmod +x ./bin/ingestor ./bin/log-agent
	./bin/ingestor --addr=:8080 --dashboard=./dashboard --auto-simulate=true

# Sync dashboard assets into docs/ for GitHub Pages
pages:
	mkdir -p docs
	cp dashboard/index.html docs/index.html
	cp dashboard/styles.css docs/styles.css
	cp dashboard/app.js docs/app.js
	@echo "GitHub Pages docs/ directory synced successfully."

# Run the CLI log agent in continuous multi-service streaming mode
agent-sim:
	./bin/log-agent --endpoint=http://localhost:8080/api/v1/ingest --simulate --rate=30 --gzip

# Inject an immediate CRITICAL_ERROR burst from the CLI log agent
inject-critical:
	./bin/log-agent --endpoint=http://localhost:8080/api/v1/ingest --inject-critical --gzip

# Send a 950-log spike from the CLI log agent to trigger backpressure watermarks
spike:
	./bin/log-agent --endpoint=http://localhost:8080/api/v1/ingest --burst=950 --gzip

# Run the automated end-to-end integration test suite
test:
	@chmod +x ./bin/ingestor ./bin/log-agent ./tests/e2e_test.py
	python3 ./tests/e2e_test.py

# Compile native Go binaries when Go toolchain is installed
build-go:
	go build -o ./bin/ingestor-go ./cmd/ingestor
	go build -o ./bin/log-agent-go ./cmd/log-agent
	go test -v ./...

# Launch full external Kafka (KRaft) + ClickHouse + Go Ingestor + Go Log Agent stack
docker-up:
	docker compose up --build

