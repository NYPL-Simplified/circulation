.PHONY: help build db-session webapp-shell up up-watch start stop down test clean full-clean
.DEFAULT_GOAL := help

help:
	@echo "Usage: make [COMMAND]"
	@echo ""
	@echo "Commands:"
	@echo ""
	@echo "  Related to Local Development:"
	@echo ""
	@echo "    build            - Build the libreg_webapp and libreg_local_db images"
	@echo "    db-session       - Start a psql session as the superuser on the db container"
	@echo "    webapp-shell     - Open a shell on the webapp container"
	@echo "    up               - Bring up the local cluster in detached mode"
	@echo "    up-watch         - Bring up the local cluster, attach to webapp and scripts"
	@echo "    up-webapp        - Start the webapp, db, and elasticsearch containers in detached mode"
	@echo "    up-webapp-watch  - Start the webapp, db, and elasticsearch containers, stay attached"
	@echo "    up-full          - Bring up the local cluster in detached mode"
	@echo "    up-full-watch    - Bring up the local cluster, remains attached"
	@echo "    start            - Start a stopped cluster"
	@echo "    stop             - Stop the cluster without removing containers"
	@echo "    down             - Take down the local cluster"
	@echo "    test             - Run the python test suite on the webapp container"
	@echo "    test-x           - Run the python test suite, exit at first failure"
	@echo "    clean            - Take down the local cluster and removes the db volume"
	@echo "    full-clean       - Take down the local cluster and remove containers, volumes, and images"
	@echo ""

build:
	docker-compose build

db-session:
	docker exec -it cm_local_db psql -U postgres

webapp-shell:
	docker exec -it cm_local_webapp /bin/bash

up:
	docker-compose up -d

up-watch:
	docker-compose up webapp scripts

up-webapp:
	docker-compose up -d webapp

up-webapp-watch:
	docker-compose up webapp

up-full:
	docker-compose up -d

up-full-watch:
	docker-compose up

start:
	docker-compose start

stop:
	docker-compose stop

down:
	docker-compose down

test:
	docker exec -it cm_local_webapp /usr/local/bin/runinvenv /simplye_venv pytest tests

test-x:
	docker exec -it cm_local_webapp /usr/local/bin/runinvenv /simplye_venv pytest -x tests

clean:
	docker-compose down --volumes

full-clean:
	docker-compose down --volumes --rmi all
