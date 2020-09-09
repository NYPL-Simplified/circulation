#!/bin/bash
set -ex

# Configure uwsgi.
cp /ls_build/services/uwsgi.ini /var/www/circulation/uwsgi.ini
chown simplified:simplified /var/www/circulation/uwsgi.ini
mkdir /var/log/uwsgi
chown -R simplified:simplified /var/log/uwsgi

# Defer uwsgi service to simplified.
mkdir /etc/service/runsvdir-simplified
cp /ls_build/services/simplified_user.runit /etc/service/runsvdir-simplified/run

# Prepare uwsgi for runit.
app_home=/home/simplified
mkdir -p $app_home/service/uwsgi
cp /ls_build/services/uwsgi.runit $app_home/service/uwsgi/run
chown -R simplified:simplified $app_home/service

# Create an alias to restart the application.
touch $app_home/.bash_aliases
echo "alias restart_app=\`touch ~/circulation/uwsgi.ini\`" >> $app_home/.bash_aliases
chown -R simplified:simplified $app_home/.bash_aliases
