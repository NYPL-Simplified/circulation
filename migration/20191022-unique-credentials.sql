-- Delete all duplicate credentials before creating unique indexes.
delete from credentials where id in (select c1.id from credentials c1 join credentials c2 on c1.data_source_id = c2.data_source_id and c1.patron_id=c2.patron_id and c1.type = c2.type and (c1.collection_id = c2.collection_id or (c1.collection_id is null and c2.collection_id is null)) and c1.id < c2.id);

DO $$ 
 BEGIN
  -- Remove the unique index on (data_source_id, type, credential).
  -- We'll recreate a better version of it immediately afterwards.
  drop index if exists ix_credentials_data_source_id_type_token;

  -- If both patron_id and collection_id are null, then (data_source_id,
  -- type, credential) must be unique.
  BEGIN
   CREATE UNIQUE index ix_credentials_data_source_id_type_credential on credentials (data_source_id, type, credential) where patron_id is null and collection_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_credentials_data_source_id_type_credential already exists, leaving it alone.';
  END;

  -- If patron_id is null, then (data_source_id, type, collection_id)
  -- must be unique.
  BEGIN
   CREATE UNIQUE index ix_credentials_data_source_id_type_collection_id on credentials (data_source_id, type, collection_id) where patron_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_credentials_data_source_id_type_collection_id already exists, leaving it alone.';
  END;

  -- If collection_id is null but patron_id is not, then
  -- (data_source_id, type, patron_id) must be unique.
  BEGIN
   CREATE UNIQUE index ix_credentials_data_source_id_type_patron_id on credentials (data_source_id, type, patron_id) where collection_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_credentials_data_source_id_type_patron_id already exists, leaving it alone.';
  END;

  -- If patron_id is null but collection_id is not, then
  -- (data_source_id, type, collection_id) must be unique.
  -- (At the moment this never happens.)
  BEGIN
   CREATE UNIQUE index ix_credentials_data_source_id_type_collection_id on credentials (data_source_id, type, collection_id) where patron_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_credentials_data_source_id_type_collection_id already exists, leaving it alone.';
  END;

  -- If both patron_id and collection_id have values, then
  -- (data_source_id, type, patron_id, collection_id) must be unique.
  BEGIN
   CREATE UNIQUE index ix_credentials_data_source_id_type_patron_id_collection_id on credentials (data_source_id, type, patron_id, collection_id);
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'Index ix_credentials_data_source_id_type_patron_id_collection_id already exists, leaving it alone.';
  END;

 END;
$$;
