alter table equivalents add column enabled boolean default true;
CREATE INDEX ix_equivalents_enabled ON equivalents USING btree (enabled);

-- Delete the recursive_equivalents function -- it will be recreated in
-- its new form when the app server starts up.
DROP FUNCTION IF EXISTS fn_recursive_equivalents(int, int, double precision, int);
