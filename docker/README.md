# Circulation Docker
This directory contains `Dockerfile`s and a `docker-compose.yml` file for Library Simplified's [Circulation Manager](https://github.com/NYPL-Simplified/circulation_manager).

#### Contents:
- What is the Circulation Manager?
- Docker Images
- Using This Image
- Environment Variables
- Building New Images
- Contributing
- License

---

## What is the Circulation Manager?

The Circulation Manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

## Docker Images

The Dockerfiles in this directory create two distinct but necessary containers to deploy the Circulation Manager:
  - `circ-webapp` (**deprecated:** `circ-deploy`): a container that launches the API and admin interface [using Nginx and uWSGI](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Nginx-&-uWSGI)
  - `circ-scripts`: a container that schedules and runs important cron jobs at recommended intervals
  - `circ-exec`: a container similar to `circ-scripts` but only runs one job and then stops

To avoid database lockups, `circ-scripts` should be deployed as a single instance.

## Using These Images

You will need **a PostgreSQL instance URL** in the format `postgres://[username]:[password]@[host]:[port]/[database_name]`. Check the `./docker-compose.yml` file for an example. With this URL, you can create containers for both the web application (`circ-webapp`) and for the background cron jobs that import and update books and otherwise keep the app running smoothly (`circ-scripts`). Either container can be used to initialize or migrate the database. During the first deployment against a brand new database, the first container run can use the default `SIMPLIFIED_DB_TASK='auto'` or be run manually with `SIMPLIFIED_DB_TASK='init'`. See the "Environment Variables" section below for more information.

### circ-webapp (**deprecated name:** circ-deploy)

Once the webapp Docker image is built, we can run it in a container with the following command.

```sh
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name webapp -d \
    --p 80:80 \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-webapp:2.1
```

Navigate to `http://localhost/admin` in your browser to visit the web admin for the Circulation Manager. In the admin, you can add or update configuration information. If you have not yet created an admin authorization protocol before, you'll need to do that before you can set other configuration.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

### circ-scripts

Once the scripts Docker image is built, we can run it in a container with the following command.

```sh
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name scripts -d \
    -e TZ='YOUR_TIMEZONE_STRING' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-scripts:2.1
```

Using `docker exec -it scripts /bin/bash` in your console, navigate to `/var/log/simplified` in the container. After 5-20 minutes, you'll begin to see log files populate that directory.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

### circ-exec

This image builds containers that will run a single script and stop. It's useful in conjunction with a tool like Amazon ECS Scheduled Tasks, where you can run script containers on a cron-style schedule.

Unlike the `circ-scripts` image, which runs constantly and executes every possible maintenance script--whether or not your configuration requires it--`circ-exec` offers more nuanced control of your Library Simplified Circulation Manager jobs. The most accurate place to look for recommended jobs and their recommended frequencies is [the existing `circ-scripts` crontab](https://github.com/NYPL-Simplified/circulation/blob/main/docker/services/simplified_crontab).

Because containers based on `circ-exec` are built, run their job, and are destroyed, it's important to configure an external log aggregator to find &#42;.log files in `/var/log/simplified/${SIMPLIFIED_SCRIPT_NAME}.log`.

```sh
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name search_index_refresh -it \
    -e SIMPLIFIED_SCRIPT_NAME='refresh_materialized_views' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-exec:2.1
```

## Environment Variables

Environment variables can be set with the `-e VARIABLE_KEY='variable_value'` option on the `docker run` command. `SIMPLIFIED_PRODUCTION_DATABASE` is the only required environment variable.

### `SIMPLIFIED_DB_TASK`

*Optional.* Performs a task against the database at container runtime. Options are:
  - `auto` : Either initializes or migrates the database, depending on if it is new or not. This is the default value.
  - `ignore` : Does nothing.
  - `init` : Initializes the app against a brand new database. If you are running a circulation manager for the first time ever, use this value to set up an Elasticsearch alias and account for the database schema for future migrations.
  - `migrate` : Migrates an existing database against a new release. Use this value when switching from one stable version to another.

### `SIMPLIFIED_PRODUCTION_DATABASE`

*Required.* The URL of the production PostgreSQL database for the application.

### `SIMPLIFIED_TEST_DATABASE`

*Optional.* The URL of a PostgreSQL database for tests. This optional variable allows unit tests to be run in the container.

### `TZ`

*Optional. Applies to `circ-scripts` only.* The time zone that cron should use to run scheduled scripts--usually the time zone of the library or libraries on the circulation manager instance. This value should be selected according to [Debian-system time zone options](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). This value allows scripts to be run at ideal times.

### `UWSGI_PROCESSES`

*Optional.* The number of processes to use when running uWSGI. This value can be updated in `docker-compose.yml` or added directly in `Dockerfile.webapp`. Defaults to 6.

### `UWSGI_THREADS`

*Optional.* The number of threads to use when running uWSGI. This value can be updated in `docker-compose.yml` or added directly in `Dockerfile.webapp`. Defaults to 2.

## Building new images

If you plan to work with stable versions of the Circulation Manager, we strongly recommend using the latest stable versions of circ-webapp and circ-scripts [published to Docker Hub](https://hub.docker.com/u/nypl/). However, there may come a time in development when you want to build Docker containers for a particular version of the Circulation Manager. If so, please use the instructions below.

We recommend you install at least version 18.06 of the Docker engine and version 1.24 of Docker Compose.

### `.webapp` and `.scripts` images

Determine which image you would like to build and update the tag and `Dockerfile` listed below accordingly.

```sh
$ docker build --build-arg version=YOUR_DESIRED_BRANCH_OR_COMMIT \
    --tag circ-scripts:development \
    --file Dockerfile.scripts \
    --no-cache .
```

You must run this command with the `--no-cache` option or the code in the container will not be updated from the last build, defeating the purpose of the build and enhancing overall confusion. Feel free to change the image tag as you like.

That's it! Run your containers as detailed in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker). Keep in mind that you may need to run migrations or configuration if you are using an existing version of the database.

### Local Testing With docker-compose

The entirety of the setup described in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker) can be run at once using `version=$YOUR_DESIRED_BRANCH_OR_COMMIT docker-compose up`. This can be great for locally testing feature branches and/or the success of new Docker builds.

[This reference](https://docs.docker.com/compose/reference/up/) has a lot of fantastic information about options and settings for `docker-compose up`, but `-d` will run the containers in the background. [`docker-compose run`](https://docs.docker.com/compose/reference/run/) allows you to run the application with commands and settings other than those set in the Dockerfiles, to further support testing.

If you're using Docker for Mac, keep an eye on the size of your /Users/[your-machine-username]/Library/Containers/com.docker.docker/Data/com.docker.driver.amd64-linux/Docker.qcow2 file, which can get quite large during local testing. Regularly deleting it will remove all existing containers but also avoid slowdowns from its ballooning size.

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-webapp/).)

## License

```
Copyright © 2021 The New York Public Library, Astor, Lenox, and Tilden Foundations

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
