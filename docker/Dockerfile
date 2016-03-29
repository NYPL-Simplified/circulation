FROM python:2.7

MAINTAINER Library Simplified <info@librarysimplified.org>

# Development libraries we'll need on top of python:2.7
RUN apt-get update && apt-get install -y --no-install-recommends \
        less nano \
        python-nose \
        git \
        libjpeg-dev \
        nginx

# Pull down the LS circulation manager and core submodule.
WORKDIR /var/www
RUN git clone https://github.com/NYPL-Simplified/circulation.git
WORKDIR circulation
RUN git submodule update --init --recursive

# Set up the virtual environment and install python libraries
RUN virtualenv env && \
    echo "export SIMPLIFIED_CONFIGURATION_FILE=\"/var/www/circulation/config.json\"" >> env/bin/activate
RUN /bin/bash -c 'source env/bin/activate && pip install -r requirements.txt && python -m textblob.download_corpora'

# Set up Nginx & UWSGI
RUN rm /etc/nginx/sites-enabled/default
COPY nginx.conf /etc/nginx/conf.d/circulation.conf
COPY uwsgi.ini uwsgi.ini

EXPOSE 80

VOLUME /var/www/circulation
ENTRYPOINT ["/bin/bash"]
