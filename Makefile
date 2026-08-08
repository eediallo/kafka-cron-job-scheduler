.PHONY: build up down logs restart

build:
	docker compose build
up:
	docker compose -f conduktor-kafka-single.yml up --build -d

down:
	docker compose -f conduktor-kafka-single.yml down -v

logs:
	docker compose -f conduktor-kafka-single.yml logs -f producer consumer-cluster-a-main consumer-cluster-a-retry consumer-cluster-b-main consumer-cluster-b-retry

restart: down up