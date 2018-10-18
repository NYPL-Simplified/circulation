-- The target ages of works were being incorrectly indexed.
-- Reindex all works that have a nontrivial target age.
delete from workcoveragerecords where operation='update-search-index' and work_id in (select id from works where target_age not in ('[,]', '[18,]'));
