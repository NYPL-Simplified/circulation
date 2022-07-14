###############################################################################
# README!
#
#     Dockerfile
#       |__ lcpencrypt
#       |
#       |__ cm_local_db
#       |
#       |__ circulation_base
#           |__ cm_webapp_base
#           |   |__ cm_webapp_local    <-- Local Webapp
#           |   |__ cm_webapp_active   <-- Deployable Webapp
#           |
#           |__ cm_scripts_base
#           |   |__ cm_scripts_local   <-- Local Script Runner
#           |   |__ cm_scripts_active  <-- Deployable Script Runner
#           |
#           |__ cm_exec_base
#               |__ cm_exec_local      <-- Local Exec
#               |__ cm_exec_active     <-- Deployable Exec
#
###############################################################################

###############################################################################
## lcpencrypt - stage to build lcpencrypt, to copy out into other stages
###############################################################################

FROM golang AS lcpencrypt
LABEL maintainer="Library Simplified <info@librarysimplified.org>"
RUN go install -v github.com/readium/readium-lcp-server/lcpencrypt@latest

###############################################################################
## cm_local_db - standalone stage to build a postgres server for local dev
###############################################################################

FROM postgres:12.8-alpine AS cm_local_db

# Set the PG superuser credentials. Low-security creds are fine, since this
# is only for local development, and does not have network accessible ports.
ENV POSTGRES_PASSWORD="password"
ENV POSTGRES_USER="postgres"

# The postgres image has a directory, /docker-entrypoint-initdb.d, where you can
# put startup scripts that will run when the container is started. However, they
# will run IF AND ONLY IF the postgres data directory is empty! So once you have
# established a persistent data store in a volume, these scripts will only run 
# again if you delete the volume, or otherwise get rid of the data directory.
COPY ./docker/localdev_postgres_init.sh /docker-entrypoint-initdb.d/localdev_postgres_init.sh

###############################################################################
## circulation_base - elements common to webapp, scripts, and exec images,
#                     both for local development and remotely deployed images
#
# Notes:
# 
#   * Logs for various pieces of the Circ. Manager will be in /var/log/simplified
#
#   * We create a user, 'simplified', to be the non-root user we step down to
#
#   * We install NodeJS from the Nodesource packages, which lets us use Node 10,
#     and avoids dependency conflicts between node and libxmlsec1 over the SSL
#     library version that we'll get via system packages.
#
###############################################################################

FROM ubuntu:22.04 as circulation_base

ARG DEBIAN_FRONTEND="noninteractive"
ARG NODESOURCE_KEYFILE="https://deb.nodesource.com/gpgkey/nodesource.gpg.key"

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 871920D1991BC93C

# Install system level dependencies
RUN apt-get update
RUN apt-get install --yes --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    gnupg1 \
    gnupg2
RUN curl -sSL ${NODESOURCE_KEYFILE} | apt-key add - \
 && echo "deb https://deb.nodesource.com/node_14.x jammy main" >> /etc/apt/sources.list.d/nodesource.list \
 && echo "deb-src https://deb.nodesource.com/node_14.x jammy main" >> /etc/apt/sources.list.d/nodesource.list
RUN apt-get update
RUN apt-get install --yes --no-install-recommends \
    build-essential \
    pkg-config \
    software-properties-common \
    language-pack-en \
    git \
    python3.10 \
    python3-dev \
    python3-setuptools \
    python3-venv \
    python3-pip \
    libpcre3 \
    libpcre3-dev \
    libffi-dev \
    libjpeg-dev \
    logrotate \
    nodejs \
    libssl-dev \
    libpq-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    libxml2-dev \
 && locale-gen en_US \
 && update-locale LANG=en_US.UTF-8 LC_CTYPE=en_US.UTF-8
RUN apt-get clean --yes
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV LANG="en_US.UTF-8"
ENV LC_CTYPE="en_US.UTF-8"

# Create simplified group and user, and log directory
RUN groupadd --gid 1000 simplified \
 && useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home --home-dir /home/simplified simplified \
 && mkdir -p /var/log/simplified

WORKDIR /home/simplified/circulation

# The virtualenv should be outside the application root, so that we can more
# easily use a host mount of the codebase without interfering with any virtualenv
# that may be present on the host.
ENV SIMPLIFIED_VENV /simplified_venv

# Set up for installing Python dependencies, by creating a virtualenv and updating
# the installation tools. Also, install a pinned version of the NLTK corpus, to avoid
# having to re-download / re-install that if other Python dependencies change.
RUN python3 -m venv ${SIMPLIFIED_VENV} \
 && ${SIMPLIFIED_VENV}/bin/python3 -m pip install -U pip setuptools \
 && ${SIMPLIFIED_VENV}/bin/python3 -m pip install textblob==0.15.3 \
 && ${SIMPLIFIED_VENV}/bin/python3 -m textblob.download_corpora \
 && mv /root/nltk_data /usr/lib

# Copy over the Python requirements files
COPY --chown=simplified:simplified ./requirements*.txt ./

# Keep there from being a clash between dm.xmlsec and libssl.
ENV CPPFLAGS="-DXMLSEC_NO_XKMS=1"

# Install the Python dependencies
RUN ${SIMPLIFIED_VENV}/bin/python3 -m pip install -U wheel pip setuptools \
 && ${SIMPLIFIED_VENV}/bin/python3 -m pip install -r ./requirements.txt

# Make sure we rotate our logs appropriately
COPY docker/logrotate.conf /etc/logrotate.conf

RUN chmod 644 /etc/logrotate.conf \
 && rm -rf /etc/logrotate.d/dpkg

# Copy over the lcpencrypt executable from its builder stage
COPY --from=lcpencrypt /go/bin/lcpencrypt /go/bin/lcpencrypt

# Bring in a helper script that simplifies activating the virtualenv prior
# to running a command, without activating it for the parent process.
COPY ./docker/runinvenv /usr/local/bin/runinvenv

# Copy over the script we'll use in all images as the ENTRYPOINT, which we'll
# pass stage/image specific CMD values to set image-specific behavior.
COPY --chown=simplified:simplified docker/docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

###############################################################################
## cm_webapp_base - elements common to cm_webapp_local and cm_webapp_active
#
# Notes:
#
#     * To serve web traffic, we'll install Nginx (which will act as a reverse
#       proxy), and gunicorn, which will be the WSGI server that runs the webapp
#
#     * We install supervisor via the system Python, not the virtualenv. It
#       doesn't need access to other packages in the virtualenv, and it's a
#       deployment dependency, not a run time dependency of the app itself,
#       so better to keep it (and its own dependencies) out of the virtualenv.
#
###############################################################################

FROM circulation_base AS cm_webapp_base

# Install and configure Nginx, Gunicorn, and Supervisord
RUN apt-get update \
 && apt-get install --yes --no-install-recommends nginx-light \
 && python3 -m pip install supervisor \
 && mkdir -p /var/log/supervisord \
 && chown simplified:simplified /var/log/supervisord \
 && ${SIMPLIFIED_VENV}/bin/python3 -m pip install gunicorn \
 && mkdir -p /var/log/gunicorn \
 && chown simplified:simplified /var/log/gunicorn \
 && apt-get clean --yes \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./docker/nginx.conf /etc/nginx/nginx.conf
COPY ./docker/gunicorn.conf.py /etc/gunicorn/gunicorn.conf.py

# Bring over the supervisor config we'll defer execution to in the
# docker-entrypoint.sh script after any initialization logic is complete
COPY ./docker/supervisord-webapp.ini /etc/supervisord.conf

# Create a static version of the front end to serve
COPY ./api/admin/package*.json ./

ENV SIMPLIFIED_STATIC_DIR /simplified_static

RUN set -ex \
 && mkdir -p /tmp/npm_build \
 && cp ./package*.json /tmp/npm_build \
 && npm install --prefix /tmp/npm_build \
 && mkdir -p ${SIMPLIFIED_STATIC_DIR} \
 && cp /tmp/npm_build/node_modules/simplified-circulation-web/dist/* ${SIMPLIFIED_STATIC_DIR} \
 && chown -R simplified:simplified ${SIMPLIFIED_STATIC_DIR} \
 && rm -rf /tmp/npm_build

# Set the value that will be passed as an argument to the entrypoint script
CMD ["webapp"]

###############################################################################
## cm_webapp_local - local dev version of webapp, relies on host mounted code
###############################################################################

FROM cm_webapp_base AS cm_webapp_local
ENV FLASK_ENV development

###############################################################################
## cm_webapp_active - self-contained version of webapp, for remote deploy
###############################################################################

FROM cm_webapp_base AS cm_webapp_active
ENV FLASK_ENV production

COPY --chown=simplified:simplified . /home/simplified/circulation/

###############################################################################
## cm_scripts_base - elements common to cm_scripts_local and cm_scripts_active
###############################################################################

FROM circulation_base AS cm_scripts_base

ENV SIMPLIFIED_STATIC_DIR /simplified_static

# By default cron is not installed in the base Ubuntu image, so we add it here.
# Also need to add a non-empty static resource directory so the app doesn't raise
# an exception on start.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends cron \
 && apt-get clean --yes \
 && mkdir -p ${SIMPLIFIED_STATIC_DIR} \
 && touch ${SIMPLIFIED_STATIC_DIR}/empty_file \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY ./docker/simplified_crontab /etc/cron.d/circulation

RUN chmod 664 /etc/cron.d/circulation \
 && crontab /etc/cron.d/circulation \
 && touch /var/log/cron.log

CMD ["scripts", "|& tee -a /var/log/cron.log 2>$1"]

###############################################################################
## cm_scripts_local - local dev version of scripts, relies on host mounted code
###############################################################################

FROM cm_scripts_base AS cm_scripts_local
ENV FLASK_ENV development

###############################################################################
## cm_scripts_active - self-contained version of scripts, for remote deploy
###############################################################################

FROM cm_scripts_base AS cm_scripts_active
ENV FLASK_ENV production

COPY --chown=simplified:simplified . /home/simplified/circulation/

###############################################################################
## cm_exec_base - elements common to cm_exec_local and cm_exec_active
###############################################################################

FROM circulation_base AS cm_exec_base
CMD ["exec"]

###############################################################################
## cm_exec_local - local dev version of exec, relies on host mounted code
###############################################################################

FROM cm_exec_base AS cm_exec_local
ENV FLASK_ENV development

###############################################################################
## cm_exec_active - self-contained version of exec, for remote deploy
###############################################################################

FROM cm_exec_base AS cm_exec_active
ENV FLASK_ENV production

COPY --chown=simplified:simplified . /home/simplified/circulation/
