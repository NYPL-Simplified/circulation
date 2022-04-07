-- These materialized views haven't been used since January 2018, and
-- don't exist in servers created after that date. We can get rid of
-- them in servers that were created before that date.
drop materialized view if exists mv_works_editions_datasources_identifiers;
drop materialized view if exists mv_works_editions_workgenres_datasources_identifiers;
