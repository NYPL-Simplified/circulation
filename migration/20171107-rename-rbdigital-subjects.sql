-- Rename former OneClick subjects to RBdigital subjects.
update subjects set type='RBdigital' where type='OneClick';
update subjects set type='RBdigital Audience' where type='OneClick Audience';
update subjects set checked=false where type like 'RBdigital';

-- Mark subjects as unchecked where classification rules have been improved
-- so that bin/repair/work_reclassify_unchecked_subjects can reclassify
-- the books that use these subjects.
update subjects set checked=false where identifier ilike 'true%crime';
update subjects set checked=false where identifier ilike 'sci%fi';
update subjects set checked=false where identifier ilike 'language arts%';
update subjects set checked=false where identifier ilike 'inspirational nonfiction';
update subjects set checked=false where identifier ilike 'women%s fiction';
update subjects set checked=false where identifier ilike 'beginning reader';
