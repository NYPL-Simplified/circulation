-- This reweights some classifications to reflect changes in the way
-- we weight data from different sources and the fact that we no longer
-- infer audience from Dewey and LCC classifications.

-- Give any Axis 360 classifications weights that are more like what
-- we see from other commercial distributors.
update classifications set weight=100 where id in (select c.id from classifications c join datasources ds on ds.id=c.data_source_id where ds.name='Axis 360' and weight=1);

-- Weight any Bibliotheca BISAC classifications similarly.
update classifications set weight=100 where id in (select c.id from classifications c join subjects s on s.id=c.subject_id join datasources ds on ds.id=c.data_source_id where ds.name='Bibliotheca' and s.type='BISAC' and c.weight=15);

-- Same for ENKI tags.
update classifications set weight=100 where id in (select c.id from classifications c join subjects s on s.id=c.subject_id join datasources ds on ds.id=c.data_source_id where ds.name='Enki' and s.type='tag' and c.weight=1);

-- Same for all classifications from RBdigital or Odilo, regardless of type.
update classifications set weight=100 where id in (select c.id from classifications c join subjects s on s.id=c.subject_id join datasources ds on ds.id=c.data_source_id where ds.name in ('RBdigital', 'Odilo'));

-- Give a weight of 1 to any schema:audience and schema:targetAge
-- classifications that came from the metadata wrangler.
update classifications set weight=1 where id in (select c.id from classifications c join datasources ds on ds.id=c.data_source_id join subjects s on s.id=c.subject_id where ds.name='Library Simplified metadata wrangler' and s.type in ('schema:audience', 'schema:targetAge') and c.weight=100);

-- Clear out the audience of all DDC and LCC subjects where were were
-- deriving audience=Adult from a lack of explicit information.
update subjects set audience=NULL where type in ('DDC', 'LCC') and audience='Adult';

-- Delete the 'classify' coverage record for every work on the site,
-- effectively forcing all works to be reclassified.
--
-- We don't technically need to delete every single 'classify' record,
-- but on a normal site we will effectively end up deleting all of
-- them, and the query to pinpoint only the ones that need to be
-- deleted takes a really long time to run.
delete from workcoveragerecords where operation='classify';
