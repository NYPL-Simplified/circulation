FROM python:2.7
MAINTAINER Library Simplified <info@librarysimplified.org>

ARG version

ENV SIMPLIFIED_DB_TASK "ignore"

# Development libraries we'll need on top of python:2.7
RUN apt-get update && apt-get install -y --no-install-recommends \
        less nano \
        python-nose \
        git \
        libjpeg-dev \
        logrotate \
        supervisor \
    && rm -rf /var/lib/apt/lists/*

# Pull down the LS circulation manager and core submodule.
WORKDIR /var/www
RUN git clone https://github.com/NYPL-Simplified/circulation.git
WORKDIR circulation

# Build the requested version of the codebase.
RUN git checkout "$version"

# Use an https link for the server_core submodule to avoid and update
# the submodule for this version.
RUN /bin/bash -c 'git config submodule.core.url \
        https://github.com/NYPL-Simplified/server_core.git \
        && git submodule update --init --recursive'

# Copy a bit o' code that you might want if you want to deploy via git push.
COPY post-receive.sample .git/hooks
# add the logrotate.conf file.
COPY logrotate.conf /etc/
COPY default_logrotate /etc/logrotate.d/

# Add a logrotate file for circulation log files.
COPY libsimple.conf /etc/logrotate.d/

# Limit write access so logrotate will use the config files.
RUN chmod 644 /etc/logrotate.conf
RUN chmod 644 /etc/logrotate.d/default_logrotate
RUN chmod 644 /etc/logrotate.d/libsimple.conf

# Create the main log directory and mount the logs.
RUN mkdir /var/log/libsimple
RUN chmod 666 /var/log/

# Remove the default supervisor configuration, which
# has a web interface we don't want, in favor of our
# own configuration files.
RUN rm /etc/supervisor/supervisord.conf
COPY supervisord.conf /etc/supervisor/
COPY cron.conf /etc/supervisor/conf.d

# Copy over script to initialize database
COPY manage_simplified_database /usr/local/bin
RUN chmod +x /usr/local/bin/manage_simplified_database

# Pass the mounted configuration file into the environment.
RUN virtualenv env && \
    echo "export SIMPLIFIED_CONFIGURATION_FILE=\"/etc/circulation/config.json\"" >> env/bin/activate

# Install required python libraries.
RUN /bin/bash -c 'source env/bin/activate && \
        pip install -r requirements.txt && \
        python -m textblob.download_corpora'
RUN mv /root/nltk_data /usr/lib/

# remove logrotate for dpkg as we will do our own in the default_logrotate.
RUN rm -rf /etc/logrotate.d/dpkg

# If you launch the container interactively with `docker run -it`,
# this is where you'll end up:
ENTRYPOINT ["/bin/bash"]
