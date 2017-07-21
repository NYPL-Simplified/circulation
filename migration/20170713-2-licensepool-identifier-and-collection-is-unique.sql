-- It used to be that LicensePool.identifier_id had to be unique. Now
-- the combination of identifier_id, data_source_id, and collection_id
-- must be unique.
ALTER TABLE licensepools DROP CONSTRAINT IF EXISTS licensepools_identifier_id_key;
ALTER TABLE licensepools DROP CONSTRAINT IF EXISTS licensepools_identifier_id_fkey;

ALTER TABLE licensepools ADD COLUMN collection_id integer;
ALTER TABLE licensepools ADD CONSTRAINT licensepools_collection_id_fkey FOREIGN KEY (collection_id) REFERENCES collections(id);
ALTER TABLE licensepools ADD CONSTRAINT licensepools_identifier_id_collection_id_key UNIQUE (identifier_id, data_source_id, collection_id);

DROP INDEX ix_licensepools_data_source_id_identifier_id;
CREATE UNIQUE INDEX ix_licensepools_collection_id_data_source_id_identifier_id on licensepools USING btree (collection_id, data_source_id, identifier_id);
