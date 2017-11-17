-- "3M" subjects can now be processed as BISAC.
update subjects set type='BISAC' where type='3M';

-- Up to this point we've been storing BISAC names in the 'identifier'
-- slot. But BISAC subjects actually have identifiers.  Bibliotheca
-- and Axis don't mention them, but other vendors do. Move all BISAC
-- identifiers into the 'name' slot.
update subjects set name=identifier where type='BISAC' and name is null;
update subjects set identifier=null where name=identifier and name not like '%0';

-- All existing 'BISAC' subjects should be rechecked with the new rules.
update subjects set checked=false where type='BISAC';

-- Old code incorrectly classified 'Fiction / Urban' as 'Urban Fiction'. 
-- All such subjects need to be reevaluated.
update subjects set checked=false where name ilike 'fiction%urban';
