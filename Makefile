# Real-Time Fraud Detection System — Makefile
# Common shortcuts. Use `make <target>`; add `-f docker-compose.yml` args as needed.

COMPOSE := docker compose
COMPOSE_APP := docker compose --profile app
COMPOSE_FULL := docker compose --profile full

.PHONY: help up up-core up-app up-full down logs psql topics reset seed

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: up-full ## Start the whole stack

up-core: ## Start only Kafka + PostgreSQL (+ topic init)
	$(COMPOSE) up -d
	@echo "Core is up. Kafka: localhost:29092 | Postgres: localhost:5432"

up-app: up-core ## Start core + producer + spark
	$(COMPOSE_APP) up -d --build

up-full: up-app ## Start everything (core + app + backend + dashboard)
	$(COMPOSE_FULL) up -d --build

down: ## Stop and remove all containers/services (keeps volumes)
	$(COMPOSE_FULL) down

reset: ## Stop everything and wipe volumes (fresh start)
	$(COMPOSE_APP) down -v

logs: ## Tail logs of all services
	$(COMPOSE_FULL) logs -f

ps: ## Show running services and health
	$(COMPOSE_FULL) ps

psql: ## Open a psql shell into frauddb
	docker exec -it fraud-postgres psql -U ${POSTGRES_USER:-fraud} -d ${POSTGRES_DB:-frauddb}

topics: ## List Kafka topics
	docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

consume: ## Dump raw messages from the transactions topic
	docker exec fraud-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic ${TOPIC_TRANSACTIONS:-transactions} --from-beginning --max-messages ${MAX_MESSAGES:-10} --timeout-ms 8000

seed: ## Run one burst of fraud-user events through the producer
	docker compose --profile app run --rm producer python produce.py --oneshot