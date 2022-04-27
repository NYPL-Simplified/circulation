# Direct Installation of the Circulation Manager

The Library Simplified Circulation Manager is a WSGI-compliant Python/Flask web application which relies on a PostgreSQL database and an ElasticSearch cluster. It is served by the `gunicorn` WSGI server, behind an Nginx reverse proxy. Though the Docker containers are based on Ubuntu Linux, the Circulation Manager should be installable on most modern operating systems.

The instructions below assume familiarity with your system's package management, and the ability to troubleshoot issues that may arise while building software from source. Installing the Circulation Manager [via Docker containers](./Development.md) is the recommended path, so this is only a loose guide (and heavily informed by the [`Dockerfile`](../Dockerfile)).

## System-Level Dependencies

### Build Dependencies

During the install process, you are likely to need your system's equivalent of the following Debian/Ubuntu packages, plus those listed below under 'Runtime Dependencies'. The build dependencies may safely be removed after installation.

* `curl`
* `ca-certificates`
* `gnupg`
* `build-essential`
* `software-properties-common`

### Runtime Dependencies

The following system packages should not be removed after installation is complete, as they are required by various parts of the application stack:

* `git`
* `python3.6`
* `python3-dev`
* `python3-setuptools`
* `python3-venv`
* `python3-pip`
* `libpcre3`
* `libpcre3-dev`
* `libffi-dev`
* `libjpeg-dev`
* `logrotate`
* `nodejs`
* `libssl-dev`
* `libpq-dev`
* `libxmlsec1-dev`
* `libxmlsec1-openssl`
* `libxml2-dev`

## Backing Services

### Database

The codebase expects to have access to a PostgreSQL installation of version 12+, with a connection string available via environment variables.

### Reverse Proxy Server

To proxy incoming requests to the `gunicorn` WSGI server, you will need to install Nginx 1.19+. Use a modified version of the [`nginx.conf`](../docker/nginx.conf) file to route requests to the WSGI server.

### Elasticsearch Cluster

The Circulation Manager uses Elasticsearch to store index data and cacheable feed representations. It is known to work with ES 6.7, and may work with other versions.

## Python Environment

The Circulation Manager currently requires Python 3.6. Once you install Python, you'll be able to set up a virtual environment to install Python dependencies into.

### Python Dependencies

Refer to the following files for the Python dependencies to install:

* [`requirements.txt`](../requirements.txt)
* [`requirements-base.txt`](../requirements-base.txt)
* [`requirements-dev.txt`](../requirements-dev.txt)

## Admin Webapp

The administrative interface is a separate application, maintained in the [`circulation-web`](https://github.com/NYPL-Simplified/circulation-web) repository.

## Operating the Stack

### Run-time Environment Variables

At a minimum you will need to supply values for the following environment variables:

* `SIMPLIFIED_VENV` - path to the project's Python virtual environment
* `SIMPLIFIED_STATIC_DIR` - path to the project's static resources folder
* `FLASK_ENV` - see the Flask documentation for [`FLASK_ENV`](https://flask.palletsprojects.com/en/2.1.x/config/#environment-and-debug-features)
* `SIMPLIFIED_PRODUCTION_DATABASE` - PostgreSQL connection string to the project's database
* `SIMPLIFIED_TEST_DATABASE` - PostgreSQL connection string to the project's test database
* `SIMPLIFIED_ELASTICSEARCH_URL` - Base URL and port of the Elasticsearch cluster that the project will access

It may also be necessary to install MinIO for the entire test suite to run correctly, which will mean also supplying values for certain environment variables such as `SIMPLIFIED_TEST_MINIO_ENDPOINT_URL`. See the [`docker-compose.yml`](../docker-compose.yml) file for specifics.
