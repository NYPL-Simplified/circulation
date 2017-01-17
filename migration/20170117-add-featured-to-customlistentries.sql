ALTER TABLE customlistentries ADD COLUMN featured boolean;
UPDATE customlistentries SET featured = 'f';
ALTER TABLE customlistentries ALTER COLUMN featured SET NOT NULL;
ALTER TABLE customlistentries ALTER COLUMN featured SET DEFAULT FALSE;
