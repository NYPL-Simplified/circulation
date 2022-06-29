-- The cachedfeeds table is likely full of junk caused by race conditions. Clear
-- it out and start fresh.
delete from cachedfeeds;
