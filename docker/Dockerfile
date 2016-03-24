FROM python:2.7

MAINTAINER Courteney Ervin <courteneyervin@nypl.org>

# Development libraries we'll need on top of python:2.7
RUN apt-get update && apt-get install -y --no-install-recommends \
        less nano \
        python-nose \
        git \
        libjpeg-dev \
        nginx

# Install Java & elasticsearch and set it up to run on boot
RUN apt-get install -y --no-install-recommends openjdk-7-jre && \
    wget -qO - https://packages.elastic.co/GPG-KEY-elasticsearch | apt-key add - && \
    echo "deb http://packages.elastic.co/elasticsearch/2.x/debian stable main" | tee -a /etc/apt/sources.list.d/elasticsearch-2.x.list && \
    apt-get install -y elasticsearch && \
    update-rc.d elasticsearch defaults 95 10

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
