.PHONY: up down create-topic gen tail test flink-submit tail-features db-consumer minio-consumer db-query

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
