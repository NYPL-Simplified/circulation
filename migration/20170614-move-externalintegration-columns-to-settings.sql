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