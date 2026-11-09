.PHONY: setup generate test api infra-up infra-up-full topics stream infra-down
setup:
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
generate:
	python3 -m src.data_generation.generate --customers 200 --products 80 --orders 1000 --out data/synthetic
test:
	python3 -m pytest tests/ -q
api:
	python3 -m uvicorn src.api.main:app --reload --port 8000
infra-up:
	docker compose up -d postgres minio
infra-up-full:
	docker compose --profile full up -d
topics:
	sh infra/kafka/create-topics.sh localhost:9092
stream:
	python3 -m src.ingestion.producers --csv data/synthetic/orders.csv --topic nexus.orders --rate 10
infra-down:
	docker compose down
