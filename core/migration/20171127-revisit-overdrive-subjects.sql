-- Mark subjects as unchecked where Overdrive classification rules
-- have been improved so that bin/work_classify_unchecked_subjects can
-- reclassify the books that use these subjects.

update subjects set checked=false where type='Overdrive' and identifier in (
'Antiquarian',
'Biology',
'Child Development',
'Drama',
'Economics',
'Gay/Lesbian',
'Genealogy',
'Latin',
'Literary Anthologies',
'Media Studies',
'Mythology',
'Outdoor Recreation',
'Poetry',
'Recovery',
'Social Media',
'Songbook',
'Text Book',
'Western',
'Writing'
);
