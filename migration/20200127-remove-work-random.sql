-- Remove indexes that haven't been used for a very long time.
drop index if exists ix_works_audience_target_age_quality_random;
drop index if exists ix_works_audience_fiction_quality_random;

-- Remove the works.random column itself.
ALTER TABLE works DROP COLUMN if exists random CASCADE;
