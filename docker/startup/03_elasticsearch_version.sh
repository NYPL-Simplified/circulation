#!/bin/bash

set -ex

WORKDIR=/var/www/circulation
su simplified <<EOF

if [[ "$SIMPLIFIED_ELASTICSEARCH_VERSION" == "1" ]] || [[ "$SIMPLIFIED_ELASTICSEARCH_VERSION" == "6" ]]; then
  # Enter the virtual environment for the application.
  source $WORKDIR/env/bin/activate;

  # Install ES libraries
  pip install -r $WORKDIR/elasticsearch-requirements-$SIMPLIFIED_ELASTICSEARCH_VERSION.txt
  echo "Using ElasticSearch $SIMPLIFIED_ELASTICSEARCH_VERSION";

else 
  echo "Unknown SIMPLIFIED_ELASTICSEARCH_VERSION '${SIMPLIFIED_ELASTICSEARCH_VERSION}' valid options are 1 or 6." && exit 127;

fi;
EOF
