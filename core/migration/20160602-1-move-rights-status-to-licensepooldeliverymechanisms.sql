ALTER TABLE licensepooldeliveries ADD COLUMN rightsstatus_id integer;
CREATE INDEX ix_licensepooldeliveries_rightsstatus_id on licensepooldeliveries USING btree (rightsstatus_id);
UPDATE licensepooldeliveries as lpd SET rightsstatus_id = lp.rightsstatus_id from licensepools as lp where lp.id = lpd.license_pool_id;
ALTER TABLE licensepools DROP COLUMN rightsstatus_id;