# Supported tags and respective `Dockerfile` links

- `2.0.0` **(untested)** [(2.0.0/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/2ca39e4/base/Dockerfile)
- `1.1.24`, `1.1`, `latest` [(1.1.24/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/363421e/base/Dockerfile)

Older versions of the Circulation Manager are not currently supported.

This image is updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

#### Contents:
- What is the Circulation Manager?
- Using This Image
  - Version 2.x
  - Version 1.1.x
- Environment Variables
- Notes on Earlier Version
- Contributing

---

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

This particular image builds a foundation for other Circulation Manager containers by acquiring the appropriate version of the codebase, creating a virtual environment, and installing required libraries. It could be successfully used as a base for new build repositories or for light development or exploratory work.

## Using This Image
### Version 2.x
You will need:
- **A PostgreSQL instance url** in the format `postgres://[username]:[password]@[host]:[port]/[database_name]`

With your PostgreSQL url, you are ready to run:
```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name base -it \
    -e SIMPLIFIED_DB_TASK='init' \
    -e SIMPLIFIED_PRODUCTION_DB='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    nypl/circ-base:2.0
```

Navigate to `http://localhost/admin` to in your browser to input or update configuration information. If you have not yet created an admin authorization protocol before, you'll need to do that before you can set other configuration.

### Version 1.1.x
You will need:
- **A configuration file** created using JSON and the keys and values described at length [here](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration). If you're unfamiliar with JSON, we highly recommend taking the time to confirm that your configuration file is valid.

With your configuration file stored on the host, you are ready to run:
```
# See the section "Environment Variables" below for more information
# about the values listed here.
$ docker run --name base -it \
    -v FULL_PATH_TO_YOUR_CONFIGURATION_FILE_DIRECTORY:/etc/circulation \
    -v SIMPLIFIED_CONFIGURATION_FILE='/etc/circulation/config.json'
    -v SIMPLIFIED_DB_TASK='migrate' \
    nypl/circ-base:1.1
```

You will need to detach from the generated TTY with `Ctrl`+P, `Ctrl`+Q to keep your container running.

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

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-base/).)
