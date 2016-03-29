FROM nypl/circulation:base
ARG timezone="US/Eastern"

MAINTAINER Library Simplified <info@librarysimplified.org>

RUN apt-get update && apt-get install -y --no-install-recommends \
        cron \
        supervisor

RUN echo $timezone > /etc/timezone

COPY run /var/www/circulation/bin
RUN chmod 744 /var/www/circulation/bin/run

# Create cron tasks & logfile
RUN touch /var/log/cron.log
COPY libsimple_crontab /etc/cron.d
