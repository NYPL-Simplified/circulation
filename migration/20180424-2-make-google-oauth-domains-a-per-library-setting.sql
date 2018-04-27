INSERT INTO externalintegrations_libraries (externalintegration_id, library_id)
SELECT e.id, l.id
FROM libraries as l join externalintegrations as e on e.protocol = 'Google OAuth';

INSERT INTO configurationsettings (external_integration_id, library_id, key, value)
SELECT cs.external_integration_id, l.id, cs.key, cs.value
FROM libraries as l join configurationsettings as cs on cs.key = 'domains';

DELETE FROM configurationsettings WHERE key = 'domains' and library_id is null;