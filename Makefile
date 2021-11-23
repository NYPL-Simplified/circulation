.PHONY: help setup install build clean full-clean \
		up up-watch up-webapp up-webapp-watch up-full up-full-watch start stop down \
		db-session webapp-shell webapp-py-repl scripts-shell scripts-py-repl test test-x \
		active-build active-up active-up-watch active-test active-test-x active-down active-clean

.DEFAULT_GOAL := help

compose_file = docker-compose.yml
ifeq ($(shell uname -m),arm64)
	compose_file = docker-compose.arm64.yml
endif
compose_command = docker-compose --file $(compose_file)

help:
	@echo "Usage: make [COMMAND]"
	@echo ""
	@echo "Commands:"
	@echo ""
	@echo "    help             - Display this help text"
	@echo ""
	@echo "  Local Development, Setup and Teardown:"
	@echo ""
	@echo "    setup            - Initialize submodule(s)"
	@echo "    install          - Run 'setup', then 'build' make recipes"
	@echo "    build            - Build the local Docker images"
	@echo "    clean            - Take down the local cluster and removes the db volume"
	@echo "    full-clean       - Take down the local cluster and remove containers, volumes, and images"
	@echo ""
	@echo "  Local Development, Cluster Control:"
	@echo ""
	@echo "    up               - Bring up the local cluster in detached mode"
	@echo "    up-watch         - Bring up the local cluster, attach to webapp and scripts"
	@echo "    up-webapp        - Start the webapp, db, and elasticsearch containers in detached mode"
	@echo "    up-webapp-watch  - Start the webapp, db, and elasticsearch containers, stay attached"
	@echo "    up-full          - Bring up the local cluster in detached mode"
	@echo "    up-full-watch    - Bring up the local cluster, remains attached"
	@echo "    start            - Start a stopped cluster"
	@echo "    stop             - Stop the cluster without removing containers"
	@echo "    down             - Take down the local cluster"
	@echo ""
	@echo "  Local Development, Interacting with Running Containers:"
	@echo ""
	@echo "    db-session       - Start a psql session as the superuser on the db container"
	@echo "    webapp-shell     - Open a shell on the webapp container"
	@echo "    webapp-py-repl   - Start a Python repl session on the webapp container, in the venv"
	@echo "    scripts-shell    - Open a shell on the scripts container"
	@echo "    scripts-py-repl  - Start a Python repl session on the scripts container, in the venv"
	@echo "    test             - Run the python test suite on the webapp container"
	@echo "    test-x           - Run the python test suite, exit at first failure"
	@echo ""
	@echo "  CI/CD, building 'active' images for deployment:"
	@echo ""
	@echo "    active-build     - TODO"
	@echo "    active-up        - TODO"
	@echo "    active-up-watch  - TODO"
	@echo "    active-test      - TODO"
	@echo "    active-test-x    - TODO"
	@echo "    active-down      - TODO"
	@echo "    active-clean     - TODO"
	@echo ""

##############################################################################
# Setup and Teardown Recipes
##############################################################################

setup:
	git submodule init && git submodule update

build:
	$(compose_command) build

install: setup build

clean:
	$(compose_command) down --volumes

full-clean:
	$(compose_command) down --volumes --rmi all

##############################################################################
# Cluster Control Recipes
##############################################################################

up:
	$(compose_command) up -d

up-watch:
	$(compose_command) up webapp scripts

up-webapp:
	$(compose_command) up -d webapp

up-webapp-watch:
	$(compose_command) up webapp

up-full:
	$(compose_command) up -d

up-full-watch:
	$(compose_command) up

start:
	$(compose_command) start

stop:
	$(compose_command) stop

down:
	$(compose_command) down

##############################################################################
# Interacting with Running Containers Recipes
##############################################################################

db-session:
	docker exec -it cm_local_db psql -U postgres

webapp-shell:
	docker exec -it cm_local_webapp /bin/bash

webapp-py-repl:
	docker exec -it cm_local_webapp /bin/bash -c 'source $$SIMPLIFIED_VENV/bin/activate && python3'

scripts-shell:
	docker exec -it cm_local_scripts /bin/bash

scripts-py-repl:
	docker exec -it cm_local_scripts /bin/bash -c 'source $$SIMPLIFIED_VENV/bin/activate && python3'

test:
	docker exec -it --env TESTING=1 cm_local_webapp /usr/local/bin/runinvenv /simplified_venv pytest tests

test-x:
	docker exec -it --env TESTING=1 cm_local_webapp /usr/local/bin/runinvenv /simplified_venv pytest -x tests

##############################################################################
# CI/CD, building 'active' images for deployment Recipes
##############################################################################

active-build:
	@echo "TODO: Implement this recipe"

active-up:
	@echo "TODO: Implement this recipe"

active-up-watch:
	@echo "TODO: Implement this recipe"

active-test:
	@echo "TODO: Implement this recipe"

active-test-x:
	@echo "TODO: Implement this recipe"

active-down:
	@echo "TODO: Implement this recipe"

active-clean:
	@echo "TODO: Implement this recipe"
