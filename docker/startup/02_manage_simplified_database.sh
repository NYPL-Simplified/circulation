#!/bin/bash
# Manages the Circulation Manager database (either initializing it, migrating
# it, or ignoring it) when the container starts and before the app launches.

set -ex

WORKDIR=/var/www/circulation
BINDIR=$WORKDIR/bin
CORE_BINDIR=$WORKDIR/core/bin

initialization_task="${BINDIR}/util/initialize_instance"
migration_task="${CORE_BINDIR}/migrate_database"

su simplified <<EOF
# Default value 'ignore' does nothing.
if ! [[ $SIMPLIFIED_DB_TASK == "ignore" ]]; then

  # Enter the virtual environment for the application.
  source $WORKDIR/env/bin/activate;

  if [[ $SIMPLIFIED_DB_TASK == "auto" ]] && [[ -f ${initialization_task} ]] \
      && [[ -f ${migration_task} ]]; then
    # Use 'auto' to initialize the database and then migrate it -- accounting
    # for either starting off an untouched database or keeping an existing one
    # up to date. This option is great for automated deployment.
    ${initialization_task} && ${migration_task};

  elif [[ $SIMPLIFIED_DB_TASK == "init" ]] && [[ -f ${initialization_task} ]]; then
    # Initialize the database with value 'init'
    ${initialization_task};

  elif [[ $SIMPLIFIED_DB_TASK == "migrate" ]] && [[ -f ${migration_task} ]]; then
    # Migrate the database with value 'migrate'
    ${migration_task};

  # Raise an error if any other value is sent
  else echo "Unknown database task '${SIMPLIFIED_DB_TASK}' requested" && exit 127;
  fi;

fi;
EOF
