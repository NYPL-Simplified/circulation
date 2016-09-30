CREATE INDEX ix_circulationevents_start_desc_nullslast ON circulationevents USING btree (start DESC NULLS LAST);
CREATE INDEX ix_circulationevents_license_pool_id ON circulationevents USING btree (license_pool_id);
CREATE INDEX ix_circulationevents_type ON circulationevents USING btree (type);
