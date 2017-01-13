ALTER TABLE customlists ADD CONSTRAINT customlists_foreign_identifier_key UNIQUE (foreign_identifier);
ALTER TABLE customlists ADD CONSTRAINT customlists_name_key UNIQUE (name);
CREATE INDEX ix_customlists_name ON customlists(name);
