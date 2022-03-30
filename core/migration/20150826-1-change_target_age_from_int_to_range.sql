DROP MATERIALIZED VIEW mv_works_editions_datasources_identifiers;
DROP MATERIALIZED VIEW mv_works_editions_workgenres_datasources_identifiers;

ALTER TABLE works add COLUMN target_age_2 NUMRANGE;
ALTER TABLE works DROP COLUMN target_age;
ALTER TABLE works RENAME COLUMN target_age_2 TO target_age;
CREATE INDEX "ix_works_target_age" ON works (target_age);

ALTER TABLE subjects add COLUMN target_age_2 NUMRANGE;
ALTER TABLE subjects DROP COLUMN target_age;
ALTER TABLE subjects RENAME COLUMN target_age_2 TO target_age;
CREATE INDEX "ix_subjects_target_age" ON subjects (target_age);
