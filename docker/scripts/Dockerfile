FROM nypl/circ-base
ENV TZ=US/Eastern
ENV LIBSIMPLE_DB_INIT ""

MAINTAINER Library Simplified <info@librarysimplified.org>

RUN apt-get update && apt-get install -y --no-install-recommends \
        cron \
    && rm -rf /var/lib/apt/lists/*

# Switch to local timezone
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Create cron tasks & logfile
RUN touch /var/log/cron.log
COPY libsimple_crontab /etc/cron.d/circulation

WORKDIR bin

# Copy over script to initialize database
COPY libsimple_init /usr/local/bin
RUN chmod +x /usr/local/bin/libsimple_init

CMD ["-c", "libsimple_init && supervisord"]
