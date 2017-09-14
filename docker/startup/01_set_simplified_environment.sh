#!/bin/bash
# Stores Circulation Manager environment variables in the virtualenv environment.
# Local environment variables are not passed into cron, so variables set at
# runtime need to be stored.

set -ex

SIMPLIFIED_ENVIRONMENT=/var/www/circulation/environment.sh

# Make sure there's a file to put environment variables into
touch $SIMPLIFIED_ENVIRONMENT

# Move all of the environment variables with Library Simplified prefixes
# into an environment file. This will allow the environment to be loaded when
# cron tasks are run, since crontab doesn't load them automatically.
printenv | \
  grep -e SIMPLIFIED -e LIBSIMPLE | \
  sed 's/^\(.*\)$/export \1/g' | \
  cat > $SIMPLIFIED_ENVIRONMENT

# Give it to the appropriate user.
chown simplified:simplified $SIMPLIFIED_ENVIRONMENT
