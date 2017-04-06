-- For OPDS Import collections, move the url to external_account_id.
UPDATE collections SET external_account_id = url WHERE protocol = 'OPDS Import';
UPDATE collections SET url = NULL WHERE protocol = 'OPDS Import';


-- Create the external integrations table, with a temporary column for the collection id.
CREATE TABLE externalintegrations (
  id SERIAL NOT NULL PRIMARY KEY,
  url varchar,
  username varchar,
  password varchar,
  collection_id integer
);


-- Create the column for the external_integration_id.
ALTER TABLE collections ADD COLUMN external_integration_id integer REFERENCES externalintegrations(id);


-- Create an external integration for each collection.
WITH insert_query AS (
  INSERT INTO externalintegrations (url, username, password, collection_id)
  SELECT c.url, c.username, c.password, c.id
  FROM collections c
  RETURNING *
  )
UPDATE collections c
SET external_integration_id = insert_query.id
FROM insert_query
WHERE c.id = insert_query.collection_id;


-- Remove the temporary column.
ALTER TABLE externalintegrations DROP COLUMN collection_id;


-- Remove the collection columns that have been moved.
ALTER TABLE collections DROP COLUMN url;
ALTER TABLE collections DROP COLUMN username;
ALTER TABLE collections DROP COLUMN password;


-- Create the external integration settings table.
CREATE TABLE externalintegrationsettings (
  id SERIAL NOT NULL PRIMARY KEY,
  external_integration_id integer REFERENCES externalintegrations(id),
  key varchar,
  value varchar,
  UNIQUE (external_integration_id, key)
);


-- Move everything from the collection settings table to the external integration settings table.
INSERT INTO externalintegrationsettings (external_integration_id, key, value)
SELECT c.external_integration_id, cs.key, cs.value
FROM collectionsettings cs
JOIN collections c ON c.id = cs.collection_id;


-- Drop the collection settings table.
DROP TABLE collectionsettings;
