-- Rename OneClick subject types to RBdigital types.
update subjects set type='RBdigital' where type='OneClick';
update subjects set type='RBdigital Audience' where type='OneClick Audience';

-- Re-weight RBdigital classifications based on observed reliability.
update classifications set weight=500 where subject_id in (select id from subjects where type='RBdigital Audience');
update classifications set weight=200 where subject_id in (select id from subjects where type='RBdigital');

-- Mark subjects as unchecked where classification rules have been improved
-- so that bin/work_classify_unchecked_subjects can reclassify
-- the books that use these subjects.
update subjects set checked=false where type like 'RBdigital';

update subjects set checked=false where identifier ilike 'true%crime';
update subjects set checked=false where identifier ilike 'sci%fi';
update subjects set checked=false where identifier ilike 'language arts%';
update subjects set checked=false where identifier ilike 'inspirational nonfiction';
update subjects set checked=false where identifier ilike 'women%s fiction';
update subjects set checked=false where identifier ilike 'beginning reader';
