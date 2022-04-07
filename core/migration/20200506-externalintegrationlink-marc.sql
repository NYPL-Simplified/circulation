-- It is possible that, since multiple storage integrations were introduced, mixed
-- use of string literals and a corresponding ExternalIntegrationLink-scoped
-- constant may have resulted in an incorrect values in the database.
-- This migration normalizes to the value of the ExternalIntegrationLink constant.

UPDATE externalintegrationslinks SET purpose='covers_mirror' WHERE purpose='covers';
UPDATE externalintegrationslinks SET purpose='books_mirror' WHERE purpose='books';
UPDATE externalintegrationslinks SET purpose='MARC_mirror' WHERE purpose='MARC';

-- Also, the Marc Export integration may now have vestigial "storage_protocol" setting,
-- which would need removal, if present.

DELETE FROM configurationsettings WHERE key='storage_protocol';
