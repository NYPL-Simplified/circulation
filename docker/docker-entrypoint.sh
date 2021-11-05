#!/bin/bash

# Note: Do NOT set the `-e` bash flag at the head of this file. It will not play
#       nicely with the db_is_ready() function, which relies on catching non-zero
#       exit codes to determine the state of the database server. The -e flag
#       interprets any non-zero return from an operation as a reason to exit.

# Env vars
SIMPLIFIED_HOME="/home/simplified"
CM_HOME="${SIMPLIFIED_HOME}/circulation"
CM_BIN_DIR="${CM_HOME}/bin"
CORE_BIN_DIR="${CM_HOME}/core/bin"

##############################################################################
# Set up the .version file if it does not already exist (with something in it)
##############################################################################

if ! [[ -s ${CM_HOME}/.version ]]; then
    pushd ${CM_HOME}
    printf "$(git describe --tags)" > ${CM_HOME}/.version
    popd
fi

##############################################################################
# Make a file that can be sourced by cron jobs to pick up SimplyE env vars
##############################################################################

SIMPLIFIED_ENV_SCRIPT=${CM_HOME}/environment.sh
touch $SIMPLIFIED_ENV_SCRIPT
printenv | grep -e 'SIMPLIFIED' -e 'LIBSIMPLE' | sed 's/^/export /' > $SIMPLIFIED_ENV_SCRIPT
chown simplified:simplified $SIMPLIFIED_ENV_SCRIPT

##############################################################################
# Wait for the database to be ready before doing more initialization work
##############################################################################

DB_READY=""
DB_READY_WAIT_SECONDS=5
COUNT=0
RETRIES=10

db_is_ready () {
    source ${CM_HOME}/env/bin/activate
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

##############################################################################
# Split behavior based on the argument passed to this script, which should be
# one of 'webapp', 'scripts', or 'exec'.
##############################################################################

while [[ $# -gt 0 ]]; do
    case "$1" in 
        webapp)
            echo "webapp"
            exit 0
            ;;
        scripts)
            source ${CM_HOME}/env/bin/activate
            db_init_script="${CM_BIN_DIR}/util/initialize_database"
            migrate_script="${CORE_BIN_DIR}/migrate_database"
            if [[ -x $db_init_script && -x $migrate_script ]]; then
                ${db_init_script} && ${migrate_script}
            ;;
        exec)
            echo "exec"
            exit 0
            ;;
        *)
            break
            ;;
    esac
done