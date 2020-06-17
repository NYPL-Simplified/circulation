#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

repo="$1"
version="$2"

apt-get update && $minimal_apt_get_install python-dev \
  python2.7 \
  python-nose \
  python-setuptools \
  gcc \
  git \
  libpcre3 \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \
  nodejs \
  npm \
  libpq-dev \
  libxmlsec1-dev \
  libxml2-dev

ln /usr/bin/nodejs /usr/bin/node

# Create a user.
useradd -ms /bin/bash -U simplified

# Get the proper version of the codebase.
mkdir /var/www && cd /var/www
git clone https://github.com/${repo}.git circulation
chown simplified:simplified circulation
cd circulation
git checkout $version

# Use https to access submodules.
git config submodule.core.url https://github.com/NYPL-Simplified/server_core.git
git config submodule.docker.url https://github.com/NYPL-Simplified/circulation-docker.git
git submodule update --init --recursive

# Add a .version file to the directory. This file
# supplies an endpoint to check the app's current version.
printf "$(git describe --tags)" > .version

# Use the latest version of pip to install a virtual environment for the app.
easy_install pip
pip install --no-cache-dir virtualenv virtualenvwrapper
virtualenv -p /usr/bin/python2.7 env

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install required python libraries.
set +x && source env/bin/activate && set -x
pip install -r requirements.txt

# Install NLTK.
python -m textblob.download_corpora
mv /root/nltk_data /usr/lib/

cd api/admin
npm install
cd ../..

# Link the repository code to /home/simplified and change permissions
su - simplified -c "ln -s /var/www/circulation /home/simplified/circulation"
chown -RHh simplified:simplified /home/simplified/circulation

# Give logs a place to go.
mkdir /var/log/simplified

# Copy scripts that run at startup.
cp /ls_build/startup/* /etc/my_init.d/
