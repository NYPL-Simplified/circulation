-- This is a reverse migration for 20210405-update-datetime-columns.sql
-- It should only be uncommented in the event that you need to deploy this fix
-- via the build system. Otherwise, you should run the commands by hand.
--
-- These statements are grouped into the same batches as
-- 20210405-update-datetime-columns.sql, and it may be possible to
-- save time by running the batches in parallel, as described in that
-- file.
--
-- Force the core migration script to run each command in this file as an individual transaction:
--   SIMPLYE_MIGRATION_TRANSACTION_PER_STATEMENT
--
-- Force the migration script to ignore this migration. Remove the following
-- line and uncomment the ALTER statements if you need this to be run automatically.
--   SIMPLYE_MIGRATION_DO_NOT_EXECUTE

-- Group 1A
--ALTER TABLE cachedmarcfiles ALTER COLUMN start_time SET DATA TYPE timestamp, ALTER COLUMN end_time SET DATA TYPE timestamp;
--ALTER TABLE complaints ALTER COLUMN "timestamp" SET DATA TYPE timestamp, ALTER COLUMN resolved SET DATA TYPE timestamp;
--ALTER TABLE coveragerecords ALTER COLUMN "timestamp" SET DATA TYPE timestamp;
--ALTER TABLE credentials ALTER COLUMN expires SET DATA TYPE timestamp;
--ALTER TABLE customlists ALTER COLUMN created SET DATA TYPE timestamp, ALTER COLUMN updated SET DATA TYPE timestamp;
--ALTER TABLE customlistentries ALTER COLUMN first_appearance SET DATA TYPE timestamp, ALTER COLUMN most_recent_appearance SET DATA TYPE timestamp;
--ALTER TABLE integrationclients ALTER COLUMN created SET DATA TYPE timestamp, ALTER COLUMN last_accessed SET DATA TYPE timestamp;
--ALTER TABLE licenses ALTER COLUMN expires SET DATA TYPE timestamp;
--ALTER TABLE licensepools ALTER COLUMN availability_time SET DATA TYPE timestamp, ALTER COLUMN last_checked SET DATA TYPE timestamp;
--ALTER TABLE measurements ALTER COLUMN taken_at SET DATA TYPE timestamp;
--ALTER TABLE patrons ALTER COLUMN last_external_sync SET DATA TYPE timestamp, ALTER COLUMN last_loan_activity_sync SET DATA TYPE timestamp;
--ALTER TABLE loans ALTER COLUMN "start" SET DATA TYPE timestamp, ALTER COLUMN "end" SET DATA TYPE timestamp;
--ALTER TABLE holds ALTER COLUMN "start" SET DATA TYPE timestamp, ALTER COLUMN "end" SET DATA TYPE timestamp;
--ALTER TABLE annotations ALTER COLUMN "timestamp" SET DATA TYPE timestamp;


-- Group 1B
--ALTER TABLE representations ALTER COLUMN fetched_at SET DATA TYPE timestamp, ALTER COLUMN mirrored_at SET DATA TYPE timestamp, ALTER COLUMN scaled_at SET DATA TYPE timestamp;

-- Group 2A
--ALTER TABLE timestamps ALTER COLUMN "start" SET DATA TYPE timestamp, ALTER COLUMN finish SET DATA TYPE timestamp;
--ALTER TABLE workcoveragerecords ALTER COLUMN "timestamp" SET DATA TYPE timestamp;
--ALTER TABLE works ALTER COLUMN last_update_time SET DATA TYPE timestamp, ALTER COLUMN presentation_ready_attempt SET DATA TYPE timestamp;

-- Group 2B

--ALTER TABLE cachedfeeds ALTER COLUMN "timestamp" SET DATA TYPE timestamp;
--ALTER TABLE circulationevents ALTER COLUMN "start" SET DATA TYPE timestamp, ALTER COLUMN "end" SET DATA TYPE timestamp;
