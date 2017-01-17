CREATE INDEX ix_customlists_name ON customlists(name);
ALTER TABLE customlists ADD CONSTRAINT customlists_foreign_identifier_data_source_id_key UNIQUE (foreign_identifier, data_source_id);
ALTER TABLE customlists ADD CONSTRAINT customlists_name_data_source_id_key UNIQUE (name, data_source_id);
