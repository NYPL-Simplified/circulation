.PHONY: help setup install build clean full-clean \
		up up-watch up-webapp up-webapp-watch up-full up-full-watch start stop down \
		db-session webapp-shell webapp-py-repl scripts-shell scripts-py-repl test test-x \
		active-build active-up active-up-watch active-test active-test-x active-down active-clean

.DEFAULT_GOAL := help

compose_file = docker-compose.yml
can_build_active_images = true
ifeq ($(shell uname -m),arm64)
	compose_file = docker-compose.arm64.yml
	can_build_active_images = false
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
	@echo "    build             - Build the local Docker images"
	@echo "    clean             - Take down the local cluster and removes the db volume"
	@echo "    full-clean        - Take down the local cluster and remove containers, volumes, and images"
	@echo ""
	@echo "  Local Development, Cluster Control:"
	@echo ""
	@echo "    up                - Bring up the local cluster in detached mode"
	@echo "    up-watch          - Bring up the local cluster, attach to webapp and scripts"
	@echo "    up-webapp         - Start the webapp, db, and elasticsearch containers in detached mode"
	@echo "    up-webapp-watch   - Start the webapp, db, and elasticsearch containers, stay attached"
	@echo "	   up-scripts        - Start the scripts, db, and elasticsearch containers in detached mode"
	@echo "	   up-scripts-watch  - Start the scripts, db, and elasticsearch containers, stay attached"
	@echo "    up-full           - Bring up the local cluster in detached mode"
	@echo "    up-full-watch     - Bring up the local cluster, remains attached"
	@echo "    start             - Start a stopped cluster"
	@echo "    stop              - Stop the cluster without removing containers"
	@echo "    down              - Take down the local cluster"
	@echo ""
	@echo "  Local Development, Interacting with Running Containers:"
	@echo ""
	@echo "    db-session        - Start a psql session as the superuser on the db container"
	@echo "    webapp-shell      - Open a shell on the webapp container"
	@echo "    webapp-py-repl    - Start a Python repl session on the webapp container, in the venv"
	@echo "    scripts-shell     - Open a shell on the scripts container"
	@echo "    scripts-py-repl   - Start a Python repl session on the scripts container, in the venv"
	@echo "    test              - Run the python test suites (./tests, core/tests)"
	@echo "    test-x            - Run the python test suites, exit at first failure"
	@echo ""
	@echo "  CI/CD, building 'active' images for deployment (usable on amd64 ONLY):"
	@echo ""
	@echo "    active-build      - Build images based on the docker-compose.cicd.yml file"
	@echo "    active-up         - Bring up the cluster from the docker-compose.cicd.yml file"
	@echo "    active-up-watch   - Bring up the cluster from the docker-compose.cicd.yml file, remain attached"
	@echo "    active-test       - Run the test suites on the active webapp container"
	@echo "    active-test-x     - Run the test suites on the active webapp container, exit on failure"
	@echo "    active-down       - Stop the cluster from the docker-compose.cicd.yml file, remove containers"
	@echo "    active-clean      - Stop the 'active'/cicd cluster, remove containers and volumes"
	@echo "    active-full-clean - Stop the 'active'/cicd cluster, remove containers, volumes, and images"
	@echo ""

##############################################################################
# Setup and Teardown Recipes
##############################################################################

build:
	$(compose_command) build --build-arg build_TZ="$(TZ)"

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

up-scripts:
	$(compose_command) up scripts -d

up-scripts-watch:
	$(compose_command) up scripts

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
	docker exec -it --env TESTING=1 cm_local_webapp /usr/local/bin/runinvenv /simplified_venv pytest tests core/tests -vv --disable-pytest-warnings

test-x:
	docker exec -it --env TESTING=1 cm_local_webapp /usr/local/bin/runinvenv /simplified_venv pytest -x tests core/tests

##############################################################################
# CI/CD, building 'active' images for deployment Recipes
##############################################################################

active-build:
ifeq ($(can_build_active_images),true)
	docker-compose --file docker-compose.cicd.yml build
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-up:
ifeq ($(can_build_active_images), true)
	docker-compose --file docker-compose.cicd.yml up -d
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-up-watch:
ifeq ($(can_build_active_images),true)
	docker-compose --file docker-compose.cicd.yml up
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-test:
ifeq ($(can_build_active_images),true)
	docker exec -it --env TESTING=1 cm_active_webapp /usr/local/bin/runinvenv /simplified_venv pytest tests core/tests
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-test-x:
ifeq ($(can_build_active_images),true)
	docker exec -it --env TESTING=1 cm_active_webapp /usr/local/bin/runinvenv /simplified_venv pytest -x tests core/tests
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-down:
ifeq ($(can_build_active_images),true)
	docker-compose --file docker-compose.cicd.yml down
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-clean:
ifeq ($(can_build_active_images),true)
	docker-compose --file docker-compose.cicd.yml down --volumes
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif

active-full-clean:
ifeq ($(can_build_active_images),true)
	docker-compose --file docker-compose.cicd.yml down --volumes --rmi all
else
	@echo "WARNING: active-* recipes can only run on amd64 machines"
endif