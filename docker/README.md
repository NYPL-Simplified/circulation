# circulation-docker
These are the Docker images for Library Simplified's [Circulation Manager](https://github.com/NYPL-Simplified/circulation_manager).

## Supported tags and respective `Dockerfile` links

- **circ-deploy:** `2.0.5`, `2.0` [(2.0.5/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/610d709/deploy/Dockerfile)
- **circ-scripts:** `2.0.5`, `2.0` [(2.0.5/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/610d709/scripts/Dockerfile)

Older versions of the Circulation Manager are not currently supported.

This image is updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

#### Contents:
- What is the Circulation Manager?
- Using This Image
  - Version 2.x
  - Version 1.1.x
- Environment Variables
- Notes on Earlier Version
- Additional Configuration
- Contributing

---

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

The Dockerfiles in this directory create two distinct but necessary containers to deploy the Circulation Manager:
  - `circ-deploy`: a container that deploys the API [using Nginx and uWSGI](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Nginx-&-uWSGI)
  - `circ-scripts`: a container that schedules and runs important cron jobs at recommended intervals

To avoid database lockups, `circ-scripts` should be deployed as a single instance.

## Using This Image

You will need **a PostgreSQL instance url** in the format `postgres://[username]:[password]@[host]:[port]/[database_name]`. With this URL, you can created containers for both the application deployment itself (`circ-deploy`) and for the background cron jobs that import and update books and otherwise keep the app running smoothly (`circ-scripts`).

### circ-deploy

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name deploy \
    -d -p 80:80 \
    -e SIMPLIFIED_DB_TASK='init' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-deploy:2.0
```

Navigate to `http://localhost/admin` to in your browser to input or update configuration information. If you have not yet created an admin authorization protocol before, you'll need to do that before you can set other configuration.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

### circ-scripts

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name scripts -d \
    -e TZ='YOUR_TIMEZONE_STRING' \
    -e SIMPLIFIED_DB_TASK='migrate' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-scripts:2.0
```

Using `docker exec -it deploy /bin/bash` in your console, navigate to `/var/log/simplified` in the container. After 5-20 minutes, you'll begin to see log files populate that directory.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

## Environment Variables

### `SIMPLIFIED_CONFIGURATION_FILE`

*Required in v1.1 only. Optional in v2.x.* The full path to configuration file in the container. Using the volume `-v` for v1.1, it should look something like `/etc/circulation/YOUR_CONFIGURATION_FILENAME.json`. In v2.x you can volume it in wherever you'd like.

Use [this documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration) to create the JSON file for your particular library's configuration. If you're unfamiliar with JSON, you can use [this JSON Formatter & Validator](https://jsonformatter.curiousconcept.com/#) to validate your configuration file.

### `SIMPLIFIED_DB_TASK`

*Required.* Performs a task against the database at container runtime. Options are:
  - `ignore` : Does nothing. This is the default value.
  - `init` : Initializes the app against a brand new database. If you are running a circulation manager for the first time every, use this value to set up an Elasticsearch alias and account for the database schema for future migrations.
  - `migrate` : Migrates an existing database against a new release. Use this value when switching from one stable version to another.

### `SIMPLIFIED_PRODUCTION_DATABASE`

*Required in v2.x only.* The URL of the production PostgreSQL database for the application.

### `SIMPLIFIED_TEST_DATABASE`

*Optional in v2.x only.* The URL of a PostgreSQL database for tests. This optional variable allows unit tests to be run in the container.

## Notes on Earlier Versions

Prior to version 1.1.23, The environment variable `LIBSIMPLE_DB_INIT` was used to initialize databases. In these versions, there was no option to migrate databases at all. Migrations against containers created with <=1.1.22 need to be run manually using the following command:
```
docker exec scripts /bin/bash -c 'source env/bin/activate && core/bin/migrate_database'
```

## Building new images

If you plan to work with stable versions of the Circulation Manager, we strongly recommend using the latest stable versions of circ-deploy and circ-scripts [published to Docker Hub](https://hub.docker.com/r/nypl/). However, there may come a time in development when you want to build Docker containers for a particular version of the Circulation Manager. If so, please use the instructions below.

### > `.deploy` and `.scripts`

Determine which container you would like to build and update the tag and Dockerfile listed below accordingly.

```sh
$ docker build --build-arg version=YOUR_DESIRED_BRANCH_OR_COMMIT \
    --tag circ-scripts:development \
    --file Dockerfile.scripts \
    --no-cache .
```

You must run this command with the `--no-cache` option or the code in the container will not be updated from the last build, defeating the purpose of the build and enhancing overall confusion. Feel free to change the image tag as you like.

That's it! Run your containers as detailed in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker). Keep in mind that you may need to run migrations or configuration if you are using an existing version of the database.

## Additional Configuration

If you would like to use different tools to handle deployment for the LS Circulation Manager, you are more than welcome to do so! We would love to support more deployment configurations; feel free to contribute any changes you may make to [the official Docker build repository](https://github.com/NYPL-Simplified/circulation-docker)!

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-deploy/).)

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
