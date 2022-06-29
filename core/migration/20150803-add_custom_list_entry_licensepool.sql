ALTER TABLE customlistentries ADD COLUMN license_pool_id integer;
CREATE INDEX "ix_customlistentries_license_pool_id" ON customlistentries (license_pool_id);
ALTER TABLE customlistentries ADD CONSTRAINT customlistentries_license_pool_id_fkey FOREIGN KEY (license_pool_id) REFERENCES licensepools(id);
