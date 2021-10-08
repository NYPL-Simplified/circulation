###############################################################################
## lcpencrypt
###############################################################################

FROM amd64/golang AS lcpencrypt

LABEL maintainer="Library Simplified <info@librarysimplified.org>"

RUN go get -v github.com/readium/readium-lcp-server/lcpencrypt

###############################################################################
## circulation_base
###############################################################################

FROM phusion/baseimage:bionic-1.0.0 as circulation_base

# TODO: Copied from /bd_build/buildconfig, which we can't source anymore
ENV MINIMAL_APT_GET_INSTALL "apt-get install -y --no-install-recommends"

COPY --from=lcpencrypt /go/bin/lcpencrypt /go/bin/lcpencrypt

# Give logs a place to go.
RUN mkdir -p /var/log/simplified

# Create a user.
RUN useradd -ms /bin/bash -U simplified
WORKDIR /home/simplified/circulation

# We'll install everything in /home/simplified/circulation, but nginx expects
# to see it i n/var/www/circulation.
RUN mkdir -p /var/www
RUN ln -s /home/simplified/circulation /var/www/circulation

# Install the nodesource nodejs package
# This lets us use node 10 and avoids dependency conflict between node and libxmlsec1 over the
# version of the ssl library that we find from package managemnet
RUN curl -sSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
RUN echo "deb https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list
RUN echo "deb-src https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list

# Add packages we need to build the app and its dependancies
RUN apt-get update
RUN $MINIMAL_APT_GET_INSTALL --no-upgrade \
  software-properties-common \
  python3.6 \
  python3-dev \
  python3-setuptools \
  python3-venv \
  python3-pip \
  gcc \
  git \
  libpcre3 \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \
  nodejs \
  libssl-dev \
  libpq-dev \
  libxmlsec1-dev \
  libxmlsec1-openssl \
  libxml2-dev

RUN python3 -m venv env

# Pass runtime environment variables to the app at runtime.
RUN touch environment.sh
ENV SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
RUN echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
         source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install required python libraries.

# Update pip and setuptools.
RUN env/bin/pip install -U pip setuptools

# Install textblob separately so we don't have to download the NLTK corpus
# every time a dependency changes.
#
# TODO: Don't hard-code the version number, get it from a standalone core/requirements-textblob.txt
# file.
RUN env/bin/pip install textblob==0.15.3

# Install the NLTK corpus
RUN env/bin/python -m textblob.download_corpora
RUN mv /root/nltk_data /usr/lib/

# Install other Python dependencies
COPY --chown=simplified:simplified ./requirements*.txt ./
COPY --chown=simplified:simplified ./core/requirements*.txt core/
RUN env/bin/pip install -r requirements.txt

# Install npm dependencies
COPY --chown=simplified:simplified ./api/admin/package*.json .
RUN npm install

# Copy scripts that run at startup.
COPY docker/startup/* /etc/my_init.d/

ENV SIMPLIFIED_DB_TASK "ignore"

###############################################################################
## circulation_logrotate
###############################################################################

from circulation_base as circulation_logrotate

VOLUME /var/log

COPY docker/services/logrotate.conf /etc
COPY docker/services/default_logrotate /etc/logrotate.d
COPY docker/services/simplified_logrotate.conf /etc/logrotate.d/simplified.conf

RUN chmod 644 /etc/logrotate.conf \
  /etc/logrotate.d/default_logrotate \
  /etc/logrotate.d/simplified.conf

# Remove logrotate for dpkg: default_logrotate overrides that behavior.
RUN rm -rf /etc/logrotate.d/dpkg

###############################################################################
## circulation_source
###############################################################################

from circulation_logrotate as circulation_source

# The images are about to diverge, so it's time to take the step that
# can't really be cached: copying the application source code into the
# image.

COPY --chown=simplified:simplified . /home/simplified/circulation

# Add a .version file to the directory. This file
# supplies an endpoint to check the app's current version.
WORKDIR /home/simplified/circulation
RUN printf "$(git describe --tags)" > .version
RUN chown simplified:simplified .version

###############################################################################
## circulation_nginx
###############################################################################

from circulation_source as circulation_nginx

# TODO: Copied from /bd_build/buildconfig, which we can't source anymore
RUN $MINIMAL_APT_GET_INSTALL nginx

# Configure nginx.
RUN rm /etc/nginx/sites-enabled/default
COPY docker/services/nginx.conf /etc/nginx/conf.d/circulation.conf
RUN echo "daemon off;" >> /etc/nginx/nginx.conf

# Prepare nginx for runit.
RUN mkdir -p /etc/service/nginx
COPY docker/services/nginx.runit /etc/service/nginx/run

###############################################################################
## circulation_uwsgi
###############################################################################

from circulation_nginx as circulation_uwsgi

# Configure uwsgi.
COPY --chown=simplified:simplified docker/services/uwsgi.ini /var/www/circulation/uwsgi.ini
RUN mkdir -p /var/log/uwsgi
RUN chown -R simplified:simplified /var/log/uwsgi

# Defer uwsgi service to simplified.
RUN mkdir -p /etc/service/runsvdir-simplified
COPY docker/services/simplified_user.runit /etc/service/runsvdir-simplified/run

# Prepare uwsgi for runit.
ENV APP_HOME=/home/simplified
RUN mkdir -p $APP_HOME/service/uwsgi
COPY docker/services/uwsgi.runit $APP_HOME/service/uwsgi/run
RUN chown -R simplified:simplified $APP_HOME/service

# Create an alias to restart the application.
RUN touch $APP_HOME/.bash_aliases
RUN echo "alias restart_app=\`touch ~/circulation/uwsgi.ini\`" >> $APP_HOME/.bash_aliases
RUN chown -R simplified:simplified $APP_HOME/.bash_aliases


###############################################################################
## circulation_exec
###############################################################################
from circulation_source as circulation_exec

WORKDIR /home/simplified/circulation/bin
CMD ["/sbin/my_init", "--skip-runit", "--quiet", "--", \
     "/bin/bash", "-c", \
     "source ../env/bin/activate && ./${SIMPLIFIED_SCRIPT_NAME}"]


###############################################################################
## circulation_scripts
###############################################################################

from circulation_source as circulation_scripts
ENV SIMPLIFIED_DB_TASK "auto"
ENV TZ=US/Eastern
CMD ["/sbin/my_init"]


# ###############################################################################
# ## circulation_webapp
# ###############################################################################

from circulation_uwsgi as circulation_webapp

EXPOSE 80

CMD ["/sbin/my_init"]
