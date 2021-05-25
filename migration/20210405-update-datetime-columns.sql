-- Change all the datetime data columns to use data type `timestamptz`. Dates
-- are already stored in UTC internally by postgres but are not returned
-- as timezone aware datetime objects.
--
-- Note that the column alteration is sufficient to set the column values to
-- the equivalent of `some_datetime_column at time zone 'utc'`, because
-- the cast adds `+00` to the value (which is the utc offset). So no additional
-- UPDATE is necessary to set the column values.
--
-- Force the core migration script to run each command in this file as an individual transaction:
--   SIMPLYE_MIGRATION_TRANSACTION_PER_STATEMENT

ALTER TABLE cachedfeeds ALTER COLUMN "timestamp" SET DATA TYPE timestamptz;
ALTER TABLE cachedmarcfiles ALTER COLUMN start_time SET DATA TYPE timestamptz, ALTER COLUMN end_time SET DATA TYPE timestamptz;
ALTER TABLE circulationevents ALTER COLUMN "start" SET DATA TYPE timestamptz, ALTER COLUMN "end" SET DATA TYPE timestamptz;
ALTER TABLE complaints ALTER COLUMN "timestamp" SET DATA TYPE timestamptz, ALTER COLUMN resolved SET DATA TYPE timestamptz;
ALTER TABLE timestamps ALTER COLUMN "start" SET DATA TYPE timestamptz, ALTER COLUMN finish SET DATA TYPE timestamptz;
ALTER TABLE coveragerecords ALTER COLUMN "timestamp" SET DATA TYPE timestamptz;
ALTER TABLE workcoveragerecords ALTER COLUMN "timestamp" SET DATA TYPE timestamptz;
ALTER TABLE credentials ALTER COLUMN expires SET DATA TYPE timestamptz;
ALTER TABLE customlists ALTER COLUMN created SET DATA TYPE timestamptz, ALTER COLUMN updated SET DATA TYPE timestamptz;
ALTER TABLE customlistentries ALTER COLUMN first_appearance SET DATA TYPE timestamptz, ALTER COLUMN most_recent_appearance SET DATA TYPE timestamptz;
ALTER TABLE integrationclients ALTER COLUMN created SET DATA TYPE timestamptz, ALTER COLUMN last_accessed SET DATA TYPE timestamptz;
ALTER TABLE licenses ALTER COLUMN expires SET DATA TYPE timestamptz;
ALTER TABLE licensepools ALTER COLUMN availability_time SET DATA TYPE timestamptz, ALTER COLUMN last_checked SET DATA TYPE timestamptz;
ALTER TABLE measurements ALTER COLUMN taken_at SET DATA TYPE timestamptz;
ALTER TABLE patrons ALTER COLUMN last_external_sync SET DATA TYPE timestamptz, ALTER COLUMN last_loan_activity_sync SET DATA TYPE timestamptz;
ALTER TABLE loans ALTER COLUMN "start" SET DATA TYPE timestamptz, ALTER COLUMN "end" SET DATA TYPE timestamptz;
ALTER TABLE holds ALTER COLUMN "start" SET DATA TYPE timestamptz, ALTER COLUMN "end" SET DATA TYPE timestamptz;
ALTER TABLE annotations ALTER COLUMN "timestamp" SET DATA TYPE timestamptz;
ALTER TABLE representations ALTER COLUMN fetched_at SET DATA TYPE timestamptz, ALTER COLUMN mirrored_at SET DATA TYPE timestamptz, ALTER COLUMN scaled_at SET DATA TYPE timestamptz;
ALTER TABLE works ALTER COLUMN last_update_time SET DATA TYPE timestamptz, ALTER COLUMN presentation_ready_attempt SET DATA TYPE timestamptz;
