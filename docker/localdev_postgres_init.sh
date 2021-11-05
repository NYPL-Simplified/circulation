#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username="$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE simplified_circulation_test;
    CREATE DATABASE simplified_circulation_dev;

    CREATE USER simplified with password 'simplified';
    grant all privileges on database simplified_circulation_dev to simplified;

    CREATE USER simplified_test with password 'simplified_test';
    grant all privileges on database simplified_circulation_test to simplified_test;

    --Add pgcrypto to any circulation manager databases.
    \c simplified_circulation_dev
    create extension pgcrypto;
    \c simplified_circulation_test
    create extension pgcrypto;
EOSQL