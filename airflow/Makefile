.PHONY: up down logs build test

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f airflow

build:
	docker-compose build

test:
	docker-compose run --rm airflow pytest -q tests
