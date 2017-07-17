FROM nypl/circ-base
MAINTAINER Library Simplified <info@librarysimplified.org>

ENV TZ=US/Eastern

RUN apt-get update && apt-get install -y --no-install-recommends \
        cron \
    && rm -rf /var/lib/apt/lists/*

# Switch to local timezone
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Create cron tasks & logfile
RUN touch /var/log/cron.log
COPY libsimple_crontab /etc/cron.d/circulation

WORKDIR bin

# Mount the logs so they can be found even if the
# container crashes.
VOLUME /var/log

CMD ["-c", "manage_simplified_database && supervisord"]
