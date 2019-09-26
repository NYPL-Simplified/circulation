-- This reweights some classifications and marks some works as needing their 
-- classifications redone using the new weights.

-- First, delete the 'classify' coverage records for certain works,
-- effectively forcing them to be reclassified. Unfortunately this will
-- probably cover most of the works on the site.
--
-- TODO: In fact, it would be simpler to delete all of the 'classify'
-- coverage records. This script would run much faster and there would
-- be no question about whether there are problems with this (rather
-- complex) query. The downside is some records would be incorrectly
-- deleted -- basically Overdrive books where we never asked the
-- metadata wrangler about them -- and there would be wasted effort on
-- the backend. But that's not a lot of books.
delete from workcoveragerecords where operation='classify' and work_id in (

 -- Which works? It's something about the primary identifier of their
 -- presentation edition.
 select w.id from works w join editions e on w.presentation_edition_id=e.id where e.primary_identifier_id in (

  -- What is it about that identifier? It has to be within a certain
  -- distance of an identifier that was handled incorrectly under
  -- the old system.

  select fn_recursive_equivalents(c.identifier_id, 3, 0.5) from classifications c join subjects s on s.id=c.subject_id join datasources ds on c.data_source_id=ds.id
  
  -- So what was wrong with the old system?

  -- We used to overweight schema:audience and schema:targetAge
  -- weights that came in from OPDS import (basically, the metadata
  -- wrangler).
  where (
   (s.type in ('schema:audience', 'schema:targetAge') and c.weight=100
    and ds.name='Library Simplified metadata wrangler'
   )

   -- We used to underweight classifications of all kinds that
   -- came from Axis 360.
   OR (ds.name='Axis 360' and c.weight=1)

   -- We used to underweight BISAC classifications that
   -- came from Bibliotheca.
   OR (ds.name='Bibliotheca' and s.type='BISAC' and c.weight=15)

   -- We used to assume that certain DDC or LCC classifications had
   -- audience=Adult, based on implicit assumptions that are often
   -- violated.
   OR (s.type in ('DDC', 'LCC') and s.audience='Adult')
  )
 )

);

-- Now that we've deleted the appropriate coverage records (which
-- required the old system to be in place), change the weights of
-- certain classifications to reflect the new system.

-- Give any Axis 360 classifications weights that are more like what
-- we see from other commercial distributors.
update classifications set weight=100 where id in (select c.id from classifications c join datasources ds on ds.id=c.data_source_id where ds.name='Axis 360' and weight=1);

-- Weight any Bibliotheca BISAC classifiers similarly.
update classifications set weight=100 where s.id in (select c.id from classifications c join subjects s on s.id=c.subject_id join datasources ds on ds.id=c.data_source_id where ds.name='Bibliotheca' and s.type='BISAC' and c.weight=15);

-- Give a weight of 1 to any schema:audience and schema:targetAge
-- classifications that came from the metadata wrangler.
update classifications set weight=1 where id in (select c.id from classifications c join datasources ds on ds.id=c.data_source_id join subjects s on s.id=c.subject_id where ds.name='Library Simplified metadata wrangler' and s.type in ('schema:audience', 'schema:targetAge') and c.weight=100);

-- Clear out the audience of all DDC and LCC subjects where were were
-- deriving audience=Adult from a lack of explicit information.
update subjects set audience=None where type in ('DDC', 'LCC') and audience='Adult';
