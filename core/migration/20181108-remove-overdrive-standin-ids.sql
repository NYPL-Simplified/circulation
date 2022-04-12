-- This script takes a belt-and-suspenders approach to removing
-- placeholder images mistakenly downloaded from Overdrive.

-- Remove the Overdrive bibliographic coverage for affected editions. The
-- Overdrive bibliographic coverage provider will try to find new images.
delete from coveragerecords where identifier_id in (select i.id from identifiers i join editions e on e.primary_identifier_id=i.id where (e.cover_full_url like '%00000000-0000-0000-0000%' or e.cover_thumbnail_url like '%00000000-0000-0000-0000%')) and data_source_id in (select ds.id from datasources ds where ds.name='Overdrive');

-- Delete work coverage records for works whose presentation editions ended up with bad cover images. They'll be refreshed and end up with no cover image. Once a better image is located, their presentation will be refreshed again.
delete from workcoveragerecords where operation='choose-edition' and work_id in (select w.id from works w where presentation_edition_id in (select id from editions where cover_full_url like '%00000000-0000-0000-0000%' or cover_thumbnail_url like '%00000000-0000-0000-0000%'));

-- Remove all hyperlinks that caused an edition to end up with a bad cover or thumbnail.
delete from hyperlinks where id in (select h.id from hyperlinks h join resources res on h.resource_id=res.id join representations rep on res.representation_id=rep.id join editions e on h.identifier_id=e.primary_identifier_id where (e.cover_full_url like '%00000000-0000-0000-0000%' or e.cover_thumbnail_url like '%00000000-0000-0000-0000%') and h.rel='http://opds-spec.org/image' and res.url like '%00000000-0000-0000-0000%');

-- Change any editions that use bad cover images so that they have no cover image.
update editions set cover_full_url=null where cover_full_url like '%00000000-0000-0000-0000%';
update editions set cover_thumbnail_url=null where cover_thumbnail_url like '%00000000-0000-0000-0000%';

