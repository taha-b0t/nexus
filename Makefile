.PHONY: setup generate test api infra-up infra-down
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
infra-down:
	docker compose down
