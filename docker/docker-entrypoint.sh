#!/bin/bash

# Note: Do NOT set the `-e` bash flag at the head of this file. It will not play
#       nicely with the db_is_ready() function, which relies on catching non-zero
#       exit codes to determine the state of the database server. The -e flag
#       interprets any non-zero return from an operation as a reason to exit.

# Env vars
SIMPLIFIED_VENV="/simplye_venv"
SIMPLIFIED_HOME="/home/simplified"
CM_HOME="${SIMPLIFIED_HOME}/circulation"
CM_BIN_DIR="${CM_HOME}/bin"
CORE_BIN_DIR="${CM_HOME}/core/bin"

##############################################################################
# Write the version info to an environment variable
##############################################################################

SIMPLIFIED_APP_VERSION="$(git -C ${CM_HOME} describe --tags)"
export SIMPLIFIED_APP_VERSION

##############################################################################
# Make a file that can be sourced by cron jobs to pick up SimplyE env vars
##############################################################################

SIMPLIFIED_ENV_SCRIPT=${SIMPLIFIED_HOME}/environment.sh
touch $SIMPLIFIED_ENV_SCRIPT
printenv | grep -e 'SIMPLIFIED' -e 'LIBSIMPLE' | sed 's/^/export /' > $SIMPLIFIED_ENV_SCRIPT
chown simplified:simplified $SIMPLIFIED_ENV_SCRIPT
export SIMPLIFIED_ENV_SCRIPT

##############################################################################
# Wait for the database to be ready before doing more initialization work
##############################################################################

DB_READY=""
DB_READY_WAIT_SECONDS=5
COUNT=0
RETRIES=10

db_is_ready () {
    source ${SIMPLIFIED_VENV}/bin/activate
    python3 > /dev/null 2>&1 <<EOF
import os,sys,psycopg2
try:
  psycopg2.connect(os.environ.get('SIMPLIFIED_PRODUCTION_DATABASE'))
except Exception:
  sys.exit(1)
sys.exit(0)
EOF
}

until [ -n "$DB_READY" ] || [ $COUNT -gt $RETRIES ]; do
    COUNT=$((COUNT+1))

    db_is_ready

    if [ $? -eq 0 ]; then
        DB_READY="true"
    else
        echo "--- Database unavailable, sleeping $DB_READY_WAIT_SECONDS seconds"
        sleep $DB_READY_WAIT_SECONDS
    fi
done

if ! [ -n "$DB_READY" ]; then
    echo "Database never became available, exiting!"
    exit 1
fi

##############################################################################
# Split behavior based on the argument passed to this script, which should be
# one of 'webapp', 'scripts', or 'exec'.
##############################################################################

while [[ $# -gt 0 ]]; do
    case "$1" in 
        webapp)
            # Defer process management to supervisor
            exec /usr/local/bin/supervisord -c /etc/supervisord.conf
            ;;
        scripts)
            # Check for migrations to run, then make cron the foreground process
            source ${SIMPLIFIED_VENV}/bin/activate
            db_init_script="${CM_BIN_DIR}/util/initialize_database"
            migrate_script="${CORE_BIN_DIR}/migrate_database"
            if [[ -x $db_init_script && -x $migrate_script ]]; then
                ${db_init_script} && ${migrate_script}
            fi
            cron -f
            ;;
        exec)
            exit 0
            ;;
        *)
            break
            ;;
    esac
done