FROM python:2.7
MAINTAINER Library Simplified <info@librarysimplified.org>

ARG version
ARG tags="t"

# Development libraries we'll need on top of python:2.7
RUN apt-get update && apt-get install -y --no-install-recommends \
        less nano \
        python-nose \
        git \
        libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# Pull down the LS circulation manager and core submodule.
WORKDIR /var/www
RUN git clone https://github.com/NYPL-Simplified/circulation.git
WORKDIR circulation

# Assume we're building for a tagged commit of circulation, but also
# allow for branches or straight-up SHAs if --build-args tags=f is used
# at build time.
RUN /bin/bash -c 'if [[ "$tags" == "t" ]];\
        then git checkout tags/"$version";\
        else git checkout "$version"; fi'

# Use an https link for the server_core submodule to avoid and update
# the submodule for this version.
RUN /bin/bash -c 'git config submodule.core.url \
        https://github.com/NYPL-Simplified/server_core.git \
        && git submodule update --init --recursive'

COPY post-receive.sample .git/hooks

# Set up the virtual environment and install python libraries
RUN virtualenv env && \
    echo "export SIMPLIFIED_CONFIGURATION_FILE=\"/etc/circulation/config.json\"" >> env/bin/activate
RUN /bin/bash -c 'source env/bin/activate && pip install -r requirements.txt && python -m textblob.download_corpora'
RUN mv /root/nltk_data /usr/lib/

ENTRYPOINT ["/bin/bash"]
