FROM nypl/circ-base

MAINTAINER Library Simplified <info@librarysimplified.org>

RUN apt-get update && apt-get install -y --no-install-recommends \
        nginx \
        supervisor \
        nodejs npm

# Initialize the database and Elasticsearch alias if
# the user indicates that this is the first time the
# container is being run.
# RUN /bin/bash -c 'source env/bin/activate && bin/util/initialize_instance'

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
CMD ["-c", "source env/bin/activate && if [ -f bin/util/initialize_instance ]; then bin/util/initialize_instance; fi && supervisord"]
