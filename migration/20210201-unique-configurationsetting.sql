-- Delete all duplicate credentials before creating unique indexes.
delete from configurationsettings where id in (select c1.id from configurationsettings c1 join configurationsettings c2 on (c1.external_integration_id = c2.external_integration_id or c1.external_integration_id is null and c2.external_integration_id is null) and (c1.library_id=c2.library_id or c1.library_id is null and c2.library_id is null) and c1.key=c2.key and c1.id < c2.id);

DO $$ 
 BEGIN
  -- Drop the ix_configurationsettings key and create a better version
  -- of it immediately afterwards.
  DROP INDEX IF EXISTS ix_configurationsettings_key;

  -- If both external_integration_id and library_id are null,
  -- then the key--the name of a sitewide setting--must be unique.
  CREATE UNIQUE INDEX ix_configurationsettings_key on configurationsettings (key) where external_integration_id is null and library_id is null;

  -- If external_integration_id is null but library_id is not,
  -- then (library_id, key) must be unique.
  BEGIN
    CREATE UNIQUE INDEX ix_configurationsettings_library_id_key on configurationsettings (library_id, key) where external_integration_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_configurationsettings_library_id_key already exists, leaving it alone.';
  END;

  -- If library_id is null but external_integration_id is not,
  -- then (external_integration_id, key) must be unique.
  BEGIN
    CREATE UNIQUE INDEX ix_configurationsettings_external_integration_id_key on configurationsettings (external_integration_id, key) where library_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_configurationsettings_external_integration_id_key already exists, leaving it alone.';
  END;

  -- If both external_integration_id and library_id have values, then
  -- (external_integration_id, library_id, key) must be unique.
  BEGIN
   CREATE UNIQUE INDEX ix_configurationsettings_external_integration_id_library_id_key on configurationsettings (external_integration_id, library_id, key);
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_configurationsettings_external_integration_id_library_id_key already exists, leaving it alone.';
  END;

 END;
$$;
