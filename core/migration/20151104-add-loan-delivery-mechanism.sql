ALTER TABLE loans ADD COLUMN fulfillment_id integer;
ALTER TABLE loans ADD CONSTRAINT loans_fulfillment_id_fkey FOREIGN KEY (fulfillment_id) REFERENCES licensepooldeliveries(id);
