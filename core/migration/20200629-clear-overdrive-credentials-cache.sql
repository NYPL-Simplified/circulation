-- Clear cached OverDrive credentials, in case any are invalid & unexpired

DELETE FROM credentials
WHERE data_source_id in (
    SELECT id FROM datasources WHERE name = 'Overdrive'
);
