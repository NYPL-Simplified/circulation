FROM nypl/circ-base

MAINTAINER Library Simplified <info@librarysimplified.org>

RUN apt-get update && apt-get install -y --no-install-recommends \
        nginx \
        supervisor \
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

# Set up supervisor to run the app
RUN mkdir -p /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/

EXPOSE 80
CMD ["-c", "/usr/bin/supervisord"]
