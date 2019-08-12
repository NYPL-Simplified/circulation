# circulation-docker
These are the Docker images for Library Simplified's [Circulation Manager](https://github.com/NYPL-Simplified/circulation_manager). They are updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

#### Contents:
- What is the Circulation Manager?
- Using This Image
- Environment Variables
- Notes on Earlier Version
- Additional Configuration
- Contributing

---

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

The Dockerfiles in this directory create two distinct but necessary containers to deploy the Circulation Manager:
  - `circ-webapp` (**deprecated:** `circ-deploy`): a container that launches the API and admin interface [using Nginx and uWSGI](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Nginx-&-uWSGI)
  - `circ-scripts`: a container that schedules and runs important cron jobs at recommended intervals

To avoid database lockups, `circ-scripts` should be deployed as a single instance.

## Using This Image

You will need **a PostgreSQL instance url** in the format `postgres://[username]:[password]@[host]:[port]/[database_name]`. With this URL, you can created containers for both the web application (`circ-webapp`) and for the background cron jobs that import and update books and otherwise keep the app running smoothly (`circ-scripts`). Either container can be used to initialize or migrate the database. During the first deployment against a brand new database, the first container run can use the default `SIMPLIFIED_DB_TASK='auto'` or be run manually with `SIMPLIFIED_DB_TASK='init'`. See the "Environment Variables" section below for mroe information.

### circ-webapp (**deprecated:** circ-deploy)

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name webapp \
    -d -p 80:80 \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-webapp:2.1
```

Navigate to `http://localhost/admin` to in your browser to input or update configuration information. If you have not yet created an admin authorization protocol before, you'll need to do that before you can set other configuration.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

### circ-scripts

```
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

Unlike the `circ-scripts` image, which runs constantly and executes every possible maintenance script--whether or not your configuration requires it--`circ-exec` offers more nuanced control of your Library Simplified Circulation Manager jobs. The most accurate place to look for recommended jobs and their recommended frequencies is [the existing `circ-scripts` crontab](https://github.com/NYPL-Simplified/circulation-docker/blob/master/services/simplified_crontab).

Because containers based on `circ-exec` are built, run their job, and are destroyed, it's important to configure an external log aggregator to find &#42;.log files in `/var/log/simplified/${SIMPLIFIED_SCRIPT_NAME}.log`.

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name refresh-materialized-views -it \
    -e SIMPLIFIED_SCRIPT_NAME='refresh_materialized_views' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-exec:2.1
```

## Environment Variables

Environment variables can be set with the `-e VARIABLE_KEY='variable_value'` option on the `docker run` command. `SIMPLIFIED_PRODUCTION_DATABASE` is the only required environment variable.

### `SIMPLIFIED_CONFIGURATION_FILE`

*Optional.* The full path to a configuration file in the container. Configuration is now held in the database and accessed via an administrative interface at `/admin`, so you probably don't need this. If you do, use [this documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration) to create the JSON file for your particular library's configuration. If you're unfamiliar with JSON, you can use [this JSON Formatter & Validator](https://jsonformatter.curiousconcept.com/#) to validate your configuration file.

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

## Building new images

If you plan to work with stable versions of the Circulation Manager, we strongly recommend using the latest stable versions of circ-webapp and circ-scripts [published to Docker Hub](https://hub.docker.com/r/nypl/). However, there may come a time in development when you want to build Docker containers for a particular version of the Circulation Manager. If so, please use the instructions below.

We recommend you install at least version 18.06 of the Docker engine and version 1.24 of Docker Compose.

### > `.webapp` and `.scripts`

Determine which container you would like to build and update the tag and Dockerfile listed below accordingly.

```sh
$ docker build --build-arg version=YOUR_DESIRED_BRANCH_OR_COMMIT \
    --tag circ-scripts:development \
    --file Dockerfile.scripts \
    --no-cache .
```

You must run this command with the `--no-cache` option or the code in the container will not be updated from the last build, defeating the purpose of the build and enhancing overall confusion. Feel free to change the image tag as you like.

That's it! Run your containers as detailed in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker). Keep in mind that you may need to run migrations or configuration if you are using an existing version of the database.

### > local testing with docker-compose

The entirety of the setup described in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker) can be run at once using `version=$YOUR_DESIRED_BRANCH_OR_COMMIT docker-compose up`. This can be great for locally testing feature branches and/or the success of new Docker builds.

[This reference](https://docs.docker.com/compose/reference/up/) has a lot of fantastic information about options and settings for `docker-compose up`, but `-d` will run the containers in the background. [`docker-compose run`](https://docs.docker.com/compose/reference/run/) allows you to run the application with commands and settings other than those set in the Dockerfiles, to further support testing.

If you're using Docker for Mac, keep an eye on the size of your /Users/courteneyervin/Library/Containers/com.docker.docker/Data/com.docker.driver.amd64-linux/Docker.qcow2 file, which can get quite large during local testing. Regularly deleting it will remove all existing containers but also avoid slowdowns from its ballooning size.

## Additional Configuration

If you would like to use different tools to handle deployment for the LS Circulation Manager, you are more than welcome to do so! We would love to support more deployment configurations; feel free to contribute any changes you may make to [the official Docker build repository](https://github.com/NYPL-Simplified/circulation-docker)!

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-webapp/).)

## License

```
Copyright © 2015 The New York Public Library, Astor, Lenox, and Tilden Foundations

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
