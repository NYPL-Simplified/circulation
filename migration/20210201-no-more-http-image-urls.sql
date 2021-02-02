-- If there are any book covers with the old, insecure URLs, we need
-- to regenerate the cached OPDS entry for every book. This will take
-- more time than running a query to locate the books that most likely
-- need to have their entries regenerated, but it will happen in the
-- background, later on, not while the site is down for a software upgrade.
--
-- The only optimization is that if there are _no_ book covers with
-- these insecure URLs, we don't need to regenerate OPDS entries at
-- all. This should be the case for most recently installed circulation
-- managers.
delete from workcoveragerecords where operation='generate-opds' and exists(select id from representations where url like 'http://book-covers.nypl.org%' limit 1);

-- Update the underlying representations used to build the OPDS entries,
-- changing insecure URLs to secure URLs.
--
-- Again, if there are no book covers with insecure URLs, don't bother.
update representations set url=replace(url, 'http://book-covers.nypl.org', 'https://covers.nypl.org') where exists(select id from representations where url like 'http://book-covers.nypl.org%' limit 1) and id in (select id from representations where url like 'http://book-covers.nypl.org%');
