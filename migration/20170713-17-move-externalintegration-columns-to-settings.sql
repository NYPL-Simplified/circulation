-- Create a setting for each external integration url.
INSERT INTO configurationsettings (external_integration_id, key, value)
SELECT e.id, 'url', e.url
FROM externalintegrations e
WHERE e.url is not null;

-- Create a setting for each external integration username.
INSERT INTO configurationsettings (external_integration_id, key, value)
SELECT e.id, 'username', e.username
FROM externalintegrations e
WHERE e.username is not null;

-- Create a setting for each external integration password.
INSERT INTO configurationsettings (external_integration_id, key, value)
SELECT e.id, 'password', e.password
FROM externalintegrations e
WHERE e.password is not null;

ALTER TABLE externalintegrations DROP COLUMN url;
ALTER TABLE externalintegrations DROP COLUMN username;
ALTER TABLE externalintegrations DROP COLUMN password;

-- Add the collection protocols to the external integrations.
UPDATE externalintegrations as e
SET protocol = c.protocol
FROM collections as c
WHERE e.id = c.external_integration_id;

-- Create an externalintegration for the Open Access Content Server.
INSERT INTO externalintegrations(protocol)
SELECT c.protocol
FROM collections c
WHERE c.external_integration_id is null;

-- Associate the OA Content Server collection with its integration.
UPDATE collections c
SET external_integration_id = e.id
FROM externalintegrations e
WHERE e.protocol = 'OPDS Import' and c.external_integration_id is null;

ALTER TABLE collections DROP COLUMN IF EXISTS protocol;

INSERT INTO configurationsettings (external_integration_id, key, value)
SELECT eis.external_integration_id, eis.key, eis.value
FROM externalintegrationsettings eis
WHERE eis.value is not null;
