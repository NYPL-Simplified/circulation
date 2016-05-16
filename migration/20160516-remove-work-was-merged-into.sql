DELETE FROM works where was_merged_into_id is not null;
ALTER TABLE works DROP COLUMN was_merged_into_id CASCADE;