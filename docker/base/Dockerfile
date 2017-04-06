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

# Copy a bit o' code that you might want if you want to deploy via git push.
COPY post-receive.sample .git/hooks

# Pass the mounted configuration file into the environment.
RUN virtualenv env && \
    echo "export SIMPLIFIED_CONFIGURATION_FILE=\"/etc/circulation/config.json\"" >> env/bin/activate

# Install required python libraries.
RUN /bin/bash -c 'source env/bin/activate && \
        pip install -r requirements.txt && \
        python -m textblob.download_corpora'
RUN mv /root/nltk_data /usr/lib/

# If you launch the container interactively with `docker run -it`,
# this is where you'll end up:
ENTRYPOINT ["/bin/bash"]
