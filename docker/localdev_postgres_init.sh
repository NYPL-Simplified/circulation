#!/bin/bash

# This script creates the test and dev databases, and users to access them,
# then adds the pgcrypto extension to both of them.
set -e

psql -v ON_ERROR_STOP=1 --username="$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE simplified_circulation_test;
    CREATE DATABASE simplified_circulation_dev;

    CREATE USER simplified WITH PASSWORD 'simplified';
    GRANT ALL PRIVILEGES ON DATABASE simplified_circulation_dev TO simplified;

    CREATE USER simplified_test WITH PASSWORD 'simplified_test';
    GRANT ALL PRIVILEGES ON DATABASE simplified_circulation_test TO simplified_test;

    \c simplified_circulation_dev
    CREATE EXTENSION pgcrypto;

    \c simplified_circulation_test
    CREATE EXTENSION pgcrypto;
EOSQL