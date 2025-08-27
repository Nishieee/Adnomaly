.PHONY: up down create-topic gen tail test flink-submit tail-features db-consumer minio-consumer db-query pgadmin feast-apply feast-ingestor feast-backfill feast-test

up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down -v

create-topic:
	docker exec -it $$(docker ps -qf name=kafka) kafka-topics \
	  --create --if-not-exists --topic clickstream --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
	docker exec -it $$(docker ps -qf name=kafka) kafka-topics \
	  --create --if-not-exists --topic features --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

gen:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	EVENTS_PER_SEC=120 HASH_SALT=$${HASH_SALT:-salt} python data/generator.py

bulk-gen:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	HASH_SALT=adnomaly_bulk python data/bulk_generator.py

tail:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python consumers/tail.py

test:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	pytest -q

flink-submit:
	# Submit the Python job using Flink's Python support
	docker exec -it $$(docker ps -qf name=flink-jobmanager) \
	  flink run -py /opt/flink/usrlib/job.py

tail-features:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python consumers/tail_features.py

db-consumer:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python consumers/db_consumer.py

minio-consumer:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python consumers/minio_consumer.py

db-query:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python tools/db_query.py

pgadmin:
	@echo "Starting pgAdmin..."
	@echo "Access pgAdmin at: http://localhost:5050"
	@echo "Email: admin@adnomaly.com"
	@echo "Password: admin123"
	@echo ""
	@echo "To connect to PostgreSQL:"
	@echo "Host: postgres"
	@echo "Port: 5432"
	@echo "Database: adnomaly"
	@echo "Username: adnomaly_user"
	@echo "Password: adnomaly_password"

feast-apply:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	cd features/feature_repo && feast apply

feast-ingestor:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python features/ingestor.py

feast-backfill:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python features/offline_backfill.py

feast-test:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python features/test_feast.py

feast-materialize:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	cd features/feature_repo && feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: build-ds train-model export-onnx eval-model

build-ds:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python model/build_dataset.py

train-model:
	. .venv/bin/activate && python model/train.py

export-onnx:
	. .venv/bin/activate && python model/export_onnx.py

eval-model:
	. .venv/bin/activate && python model/evaluate.py

.PHONY: serve docker-build docker-run curl-score

serve:
	uvicorn serving.app:app --host 0.0.0.0 --port 8080 --reload

docker-build:
	docker build -t adnomaly-serving:latest -f serving/Dockerfile .

docker-run:
	docker run --rm -p 8080:8080 adnomaly-serving:latest

curl-score:
	curl -s -X POST http://localhost:8080/v1/score \
	 -H 'content-type: application/json' \
	 -d '{"ctr_avg":0.03,"bounce_rate_avg":0.62,"event_count":350,"geo":"US","platform":"ios"}' | jq

score-run:
	python -m venv .venv && . .venv/bin/activate && \
	pip install -r requirements.txt && \
	python -m streaming.scorer

alerts-tail:
	docker exec -it $$(docker ps -qf name=kafka) kafka-console-consumer \
	  --bootstrap-server kafka:9092 \
	  --topic alerts \
	  --from-beginning \
	  --max-messages 50
