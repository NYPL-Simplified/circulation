###############################################################################
# README!
#
# TODO: Add overview of the stages in this file.
#
###############################################################################

###############################################################################
## lcpencrypt - stage to build lcpencrypt, to copy out into other stages
###############################################################################

FROM amd64/golang AS lcpencrypt

LABEL maintainer="Library Simplified <info@librarysimplified.org>"

RUN go get -v github.com/readium/readium-lcp-server/lcpencrypt

###############################################################################
## cm_local_db - standalone stage to build a postgres server for local dev
###############################################################################

FROM postgres:12.8-alpine AS cm_local_db

ENV POSTGRES_PASSWORD="password"
ENV POSTGRES_USER="postgres"

COPY ./docker/localdev_postgres_init.sh /docker-entrypoint-initdb.d/localdev_postgres_init.sh

###############################################################################
## circulation_base - elements common to webapp, scripts, and exec images,
#     both for local development and remotely deployed images
#
# Notes:
# 
#   * Logs for various pieces of SimplyE will be put in /var/log/simplified
#   * We create a user, 'simplified', to be the non-root user we step down to
#   * We create a symlink at /var/www/circulation that points to the simplified
#     user's home directory, because Nginx wants to find it under /var/www
#   * We install NodeJS from the Nodesource packages, which lets use use Node 10,
#     and avoids dependency conflicts between node and libxmlsec1 over the SSL
#     library version that we'll get via system packages.
#
###############################################################################

FROM ubuntu:18.04 as circulation_base

ARG DEBIAN_FRONTEND="noninteractive"
ARG NODESOURCE_KEYFILE="https://deb.nodesource.com/gpgkey/nodesource.gpg.key"

# Install system level dependencies
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
 && curl -sSL ${NODESOURCE_KEYFILE} | apt-key add - \
 && echo "deb https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list \
 && echo "deb-src https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list \
 && apt-get update \
 && apt-get install --yes --no-install-recommends \
    build-essential \
    software-properties-common \
    git \
    python3.6 \
    python3-dev \
    python3-setuptools \
    python3-venv \
    python3-pip \
    libpcre3 \
    libpcre3-dev \
    libffi-dev \
    libjpeg-dev \
    nodejs \
    libssl-dev \
    libpq-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    libxml2-dev \
 && apt-get clean --yes \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create group and user, and log directory
RUN groupadd --gid 1000 simplified \
 && useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home --home-dir /home/simplified simplified \
 && mkdir -p /var/log/simplified

WORKDIR /home/simplified/circulation

# Set up for installing Python dependencies, by creating a virtualenv and updating
# the installation tools. Also, install a pinned version of the NLTK corpus, to avoid
# having to re-download / re-install that if other Python dependencies change.
RUN python3 -m venv /simplye_venv \
 && /simplye_venv/bin/pip install -U pip setuptools \
 && /simplye_venv/bin/pip install textblob==0.15.3 \
 && /simplye_venv/bin/python -m textblob.download_corpora \
 && mv /root/nltk_data /usr/lib

# Copy over the Python requirements files for both CM and core
COPY --chown=simplified:simplified ./requirements*.txt ./
COPY --chown=simplified:simplified ./core/requirements*.txt ./core/

# Install the Python dependencies
RUN /simplye_venv/bin/pip install -r ./requirements.txt

RUN /simplye_venv/bin/pip uninstall uWSGI \
 && /simplye_venv/bin/pip install \
    supervisor \
    gunicorn

COPY docker/services/logrotate.conf /etc/logrotate.conf
COPY docker/services/default_logrotate /etc/logrotate.d/default_logrotate
COPY docker/services/simplified_logrotate.conf /etc/logrotate.d/simplified

RUN chmod 644 /etc/logrotate.conf /etc/logrotate.d/default_logrotate /etc/logrotate.d/simplified \
 && rm -rf /etc/logrotate.d/dpkg

# Copy over the lcpencrypt executable from its builder stage
COPY --from=lcpencrypt /go/bin/lcpencrypt /go/bin/lcpencrypt

# Copy over the script we'll use in all images as the ENTRYPOINT, which we'll
# pass stage/image specific CMD values to set image-specific behavior.
COPY --chown=simplified:simplified docker/docker-entrypoint.sh /docker-entrypoint.sh

USER simplified

ENTRYPOINT ["/docker-entrypoint.sh"]

###############################################################################
## cm_webapp_base - elements common to cm_webapp_local and cm_webapp_active
###############################################################################

FROM circulation_base AS cm_webapp_base

# Install and configure Nginx, and set up a symlink between /var/www and /home/simplified
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
    nginx-light \
 && rm -rf /etc/nginx/sites-enabled/default \
 && echo "daemon off;" >> /etc/nginx/nginx.conf \
 && mkdir -p /etc/service/nginx \
 && ln -s /home/simplified/circulation /var/www/circulation \
 && apt-get clean --yes \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY docker/services/nginx.conf /etc/nginx/conf.d/circulation.conf
COPY docker/services/nginx.runit /etc/services/nginx/run

# Configure the uwsgi server previously installed as a Python dependency
COPY --chown=simplified:simplified docker/services/uwsgi.ini /var/www/circulation/uwsgi.ini
COPY --chown=simplified:simplified docker/services/simplified_user.runit /etc/service/runsvdir-simplified/run
COPY --chown=simplified:simplified docker/services/uwsgi.runit /home/simplified/service/uwsgi/run

RUN mkdir -p /var/log/uwsgi \
 && chown -R simplified:simplified /var/log/uwsgi

CMD ["webapp"]

###############################################################################
## cm_scripts_local - local dev version of scripts, relies on host mounted code
###############################################################################

FROM circulation_base AS cm_scripts_local

###############################################################################
## cm_exec_local - local dev version of exec, relies on host mounted code
###############################################################################

FROM circulation_base AS cm_exec_local

###############################################################################
## cm_webapp_local - local dev version of webapp, relies on host mounted code
###############################################################################

FROM cm_webapp_base AS cm_webapp_local

###############################################################################
## cm_scripts_active - self-contained version of scripts, for remote deploy
###############################################################################

FROM circulation_base AS cm_scripts_active

###############################################################################
## cm_exec_active - self-contained version of exec, for remote deploy
###############################################################################

FROM circulation_base AS cm_exec_active

###############################################################################
## cm_webapp_active - self-contained version of webapp, for remote deploy
###############################################################################

FROM cm_webapp_base AS cm_webapp_active