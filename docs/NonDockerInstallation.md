# Direct Installation of the Circulation Manager

The Library Simplified Circulation Manager is a WSGI-compliant Python/Flask web application which relies on a PostgreSQL database and an ElasticSearch cluster. It is served by the `gunicorn` WSGI server, behind an Nginx reverse proxy. Though the Docker containers are based on Ubuntu Linux, the Circulation Manager should be installable on most modern operating systems.

The instructions below assume familiarity with your system's package management, and the ability to troubleshoot issues that may arise while building software from source. Installing the Circulation Manager [via Docker containers](./Development.md) is the recommended path, so this is only a loose guide (and heavily informed by the [`Dockerfile`](../Dockerfile)).

## System-Level Dependencies

### Build Dependencies

During the install process, you are likely to need your system's equivalent of the following packages, plus those listed below under 'Runtime Dependencies'. The build dependencies may safely be removed after installation.

### Runtime Dependencies

The following system packages should not be removed after installation is complete, as they are required by various parts of the application stack:

## Backing Services

### Database

### Reverse Proxy Server

To proxy incoming requests to the `gunicorn` WSGI server, you will need to install Nginx 1.19+. Use a modified version of the [`nginx.conf`](../docker/nginx.conf) file to route requests to the WSGI server.

## Python Environment

The Circulation Manager currently requires Python 3.6. Once you install Python, you'll be able to set up a virtual environment to install Python dependencies into.

### Virtual Environment

### Python Dependencies

## Admin Webapp

## Operating the Stack

### Run-time Environment Variables
