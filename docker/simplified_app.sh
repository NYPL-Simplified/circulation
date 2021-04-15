#!/bin/bash
set -e
source /bd_build/buildconfig
set -x

repo="$1"
version="$2"

if [ -z ${version} ]; then
  echo "WARN: No version specified, will build default branch.";
fi

# Install the nodesource nodejs package
# This lets us use node 10 and avoids dependency conflict between node and libxmlsec1 over the
# version of the ssl library that we find from package managemnet
curl -sSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
echo "deb https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list
echo "deb-src https://deb.nodesource.com/node_10.x bionic main" >> /etc/apt/sources.list.d/nodesource.list

# Add packages we need to build the app and its dependancies
apt-get update
$minimal_apt_get_install --no-upgrade \
  software-properties-common \
  python3.6 \
  python3-dev \
  python3-setuptools \
  python3-venv \
  python3-pip \
  gcc \
  git \
  libpcre3 \
  libpcre3-dev \
  libffi-dev \
  libjpeg-dev \
  nodejs \
  libssl-dev \
  libpq-dev \
  libxmlsec1-dev \
  libxmlsec1-openssl \
  libxml2-dev

# Create a user.
useradd -ms /bin/bash -U simplified

# Get the proper version of the codebase.
mkdir /var/www && cd /var/www
git clone https://github.com/${repo}.git circulation
chown simplified:simplified circulation
cd circulation
# git checkout $version
# temporary:
git checkout datetime-update

# Use https to access submodules.
git submodule init
git config submodule.core.url $(git config submodule.core.url | perl -p -e 's|git@(.*?):|https://\1/|g')
git submodule update --init --recursive

# temporary:
cd core && git checkout datetime-update && cd ..

# Add a .version file to the directory. This file
# supplies an endpoint to check the app's current version.
printf "$(git describe --tags)" > .version

python3 -m venv env

# Pass runtime environment variables to the app at runtime.
touch environment.sh
SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh
echo "if [[ -f $SIMPLIFIED_ENVIRONMENT ]]; then \
      source $SIMPLIFIED_ENVIRONMENT; fi" >> env/bin/activate

# Install required python libraries.
set +x && source env/bin/activate && set -x
# Update pip and setuptools.
python3 -m pip install -U pip setuptools
# Install the necessary requirements.
python3 -m pip install -r requirements.txt

# Install NLTK.
python3 -m textblob.download_corpora
mv /root/nltk_data /usr/lib/

# Go to the directory where the admin front-end application is located
# and install the necessary app dependencies.
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
