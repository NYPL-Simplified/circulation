-- The licensepools.self_hosted field has a default value, but the migration
-- script that added it to old servers didn't set one. Now old servers have
-- an invalid 'null' value in that field.
--
-- (see also 20200702-add-licensepool-self_hosted-field.sql)
ALTER TABLE licensepools ALTER COLUMN self_hosted SET DEFAULT false;
UPDATE licensepools SET self_hosted = false WHERE self_hosted is null;
