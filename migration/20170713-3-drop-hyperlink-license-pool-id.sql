-- Remove the connection between Hyperlink and LicensePool, which
-- was never used and is now in the way.
DROP INDEX IF EXISTS ix_hyperlinks_license_pool_id;
ALTER TABLE hyperlinks DROP CONSTRAINT hyperlinks_license_pool_id_fkey;
ALTER TABLE hyperlinks DROP COLUMN license_pool_id;
