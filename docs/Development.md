# Local Development for the Circulation Manager

## Installation

The recommended way to get a local development environment is via [Docker](https://www.docker.com/products/docker-desktop). If you need to perform a direct installation, please see the [Non-Docker Installation](./NonDockerInstallation.md) document.

_**Note:** The Circulation Manager containers should build and run on both amd64- and arm64-based machines. However, due to differences in the containerized version of ElasticSearch that we currently depend on, it is necessary to use a different Docker Compose file (`docker-compose.arm64.yml`) to orchestrate the cluster on arm64 machines, including those using Apple's M1 chip series. If you use the `make` commands below to control the local cluster this difference won't matter, as it will choose the correct compose file based on your machine's architecture. However, if you run the `docker-compose` commands directly on an arm64-based machine, please make sure to use the correct compose file, via `docker-compose --file docker-compose.arm64.yml <COMMAND>`._

### Docker

In addition to installing Docker Desktop (link above), you'll also need an account at [Docker Hub](https://hub.docker.com). Docker works by downloading base machine images to build on top of, and the majority of those are available from Docker Hub. However, the Docker Engine can only locate and pull images on behalf of an authenticated user.

Once you've created an account (or using an account you already have), you'll need to sign in via your Docker Desktop installation. You should be able to do that from the top right corner of the Docker Desktop dashboard window.

### Make

A [`Makefile`](../Makefile) is provided to help manage the local containers for the Circulation Manager. If you don't currently have `make` on your system, it should be available from your system's package manager (`apt`, `brew`, etc.) You can also run the recipes in the Makefile by hand, by copying and pasting the commands to your shell, if you can't get or would prefer not to install `make` itself.

### Cloning the repository and building images

The following shell commands will clone this repo and build Docker images for the Circulation Manager web application and a PostgreSQL database:

```shell
git clone https://github.com/NYPL-Simplified/circulation.git
cd ./circulation
make setup
make build
```

## Operation

Once the images are successfully built, you can start a local cluster with:

```shell
make up
```

The first time you start the cluster, the database container will run the initialization script [`localdev_postgres_init.sh`](../docker/localdev_postgres_init.sh), which creates dev and test databases, installs PostgreSQL extensions, and creates credentialed users. Since the PostgreSQL data directory is persisted in a Docker Volume, subsequent startups will not re-initialize the database. The webapp container will wait to start its webserver until the database is available and accepting connections. If you want to observe the initialization process, use `make up-watch` instead of `make up`, to keep your terminal attached to the output of the running containers.

If you have previously run a version of the containerized Circulation Manager, it is possible you'll have a Docker volume remaining to persist the PostgreSQL data directory. If that is the case, and the database in that directory is not configured correctly, you could see output from `make up-watch` like:

```Text
cm_local_db        | PostgreSQL Database directory appears to contain a database; Skipping initialization

[...]

cm_local_db        | 2021-11-19 22:20:06.563 UTC [74] FATAL:  password authentication failed for user "simplified"
cm_local_db        | 2021-11-19 22:20:06.563 UTC [74] DETAIL:  Role "simplified" does not exist.

[...]

cm_local_webapp    | --- Database unavailable, sleeping 5 seconds
```

If so, run `make clean` to remove the existing volume, then `make up-watch` again--you should see the database initialization process occur.

While the cluster is running (and after the web server has started), you should be able to access the API endpoints at `http://localhost`, and the administrative web app at `http://localhost/admin/`. The first time you attempt to sign in to the admin app a user will be created with the credentials you supply.

Other lifecycle management commands (a full list is available via `make help`):

* `make stop` / `make start` - after running `make up`, you can pause and unpause the cluster without destroying the containers
* `make down` - stops the cluster and removes the containers and virtual network
* `make clean` - stops the cluster, removes the containers and virtual network, and the db volume
* `make full-clean` - same as `make clean`, but also deletes the Docker images for the cluster

### Accessing the Containers

While the cluster is running, you can access the containers with these commands:

* `make db-session` - Starts a `psql` session on the database container as the superuser
* `make webapp-shell` - Open a bash shell on the webapp container
* `make webapp-py-repl` - Open a Python REPL session on the webapp container, in the project's virtualenv
* `make scripts-shell` - Open a bash shell on the scripts container
* `make scripts-py-repl` - Open a Python REPL session on the scripts container, in the project's virtualenv

## Testing

While you have a running cluster, you can start the test suite with:

```shell
make test
```

That will run the entire test suite via `pytest`. If you instead would like to use the `-x` option to `pytest` to exit at the first error or failure, you can use:

```shell
make test-x
```

### Running Specific Tests

You can ignore the `Makefile` and directly issue the `pytest` command to the webapp container via `docker exec`, as follows:

```shell
docker exec -it cm_local_webapp pipenv run pytest tests
```

That gives you access to all of the standard options to `pytest`, allowing variations like:

```shell
# Fail fast with -x
docker exec -it --env TESTING=1 cm_local_webapp pipenv run pytest -x tests

# Run a particular test class
docker exec -it --env TESTING=1 cm_local_webapp pipenv \run pytest tests/test_decorators.py::TestDecorators

# Run a specific test method
docker exec -it --env TESTING=1 cm_local_webapp pipenv run pytest tests/test_decorators.py::TestDecorators::test_uses_location_from_ip

# Turn off the warnings output
docker exec -it --env TESTING=1 cm_local_webapp pipenv run pytest --disable-warnings tests
```

The full set of options to the `pytest` executable is available at [https://docs.pytest.org/en/6.2.x/usage.html](https://docs.pytest.org/en/6.2.x/usage.html)

## Making Code Changes

The Docker containers for local development of the web app and script runner (`cm_local_webapp` and `cm_local_scripts`, respectively) do not have copies of the codebase in their virtualized file systems. Instead, the local directory that this repo is checked out to is made available to the container as a read-only [bind mount](https://docs.docker.com/storage/bind-mounts/), at `/home/simplified/circulation`. The setting of that bind mount occurs via `docker-compose`, and can be seen in the `webapp` and `scripts` sections of `docker-compose.yml` and `docker-compose.arm64.yml`.

Because those host mounts occur through Docker Compose, they will not be present if you directly create a container from the built images via `docker run`, unless you specify the bind mount via command line options, as with:

```shell
docker run -d --rm --mount type=bind,source="$(pwd)",target=/home/simplified/circulation \
  --entrypoint tail circulation_webapp:latest -f /dev/null
```

With the local directory as a bind mount, you should be able to make changes on your host machine, and see them reflected in the behavior of the container.
