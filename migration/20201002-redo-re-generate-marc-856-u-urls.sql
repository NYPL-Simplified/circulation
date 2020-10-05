-- Delete cached MARC delta files (those with a start time),
-- as they will contain invalid links in the 856|u.
delete from cachedmarcfiles where start_time is not null;

-- Null out the end_time for remaining cached MARC files so that the
-- coverage provider will regenerate the records as soon as possible.
update cachedmarcfiles set end_time = null where start_time is null;
