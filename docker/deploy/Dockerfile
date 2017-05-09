FROM nypl/circ-base
ENV LIBSIMPLE_DB_INIT ""

MAINTAINER Library Simplified <info@librarysimplified.org>

RUN apt-get update && apt-get install -y --no-install-recommends \
        nginx \
        nodejs npm

# Set up Nginx & UWSGI
RUN rm /etc/nginx/sites-enabled/default
COPY nginx.conf /etc/nginx/conf.d/circulation.conf
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
COPY uwsgi.ini uwsgi.ini

# Install the admin interface
WORKDIR /var/www/circulation/api/admin
RUN npm install
WORKDIR ../..

# Copy over script to initialize database
COPY libsimple_init /usr/local/bin
RUN chmod +x /usr/local/bin/libsimple_init

# Set up supervisor to run the app
COPY libsimple_uwsgi.conf /etc/supervisor/conf.d/

# Mount the logs so they can be found even if the
# container crashes.
VOLUME /var/log

EXPOSE 80
CMD ["-c", "libsimple_init && supervisord"]
