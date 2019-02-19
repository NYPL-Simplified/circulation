-- Make sure the unique constraint on licensepooldeliveries exists; it may not exist in older databases.
DO $$
  BEGIN
    alter table licensepooldeliveries add constraint if not exists licensepooldeliveries_data_source_id_identifier_id_delivery_key unique (data_source_id, identifier_id, delivery_mechanism_id, resource_id);
  EXCEPTION
    WHEN duplicate_object THEN RAISE NOTICE 'service_type already exists, not creating it.';
  END;
$$;
