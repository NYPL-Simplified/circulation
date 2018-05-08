DO $$
  BEGIN
    BEGIN
      ALTER TABLE loans ADD COLUMN integration_client_id integer;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column loans.integration_client_id already exists, not creating it.';
    END;

    BEGIN
      ALTER TABLE loans ADD CONSTRAINT loans_integration_client_id_fkey foreign key (integration_client_id) references integrationclients(id);
    EXCEPTION
      WHEN duplicate_object THEN RAISE NOTICE 'constraint loans_integration_client_id_fkey already exists, not creating it.';
    END;

    BEGIN
      ALTER TABLE holds ADD COLUMN integration_client_id integer;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column holds.integration_client_id already exists, not creating it.';
    END;

    BEGIN
      ALTER TABLE holds ADD CONSTRAINT holds_integration_client_id_fkey foreign key (integration_client_id) references integrationclients(id);
    EXCEPTION
      WHEN duplicate_object THEN RAISE NOTICE 'constraint holds_integration_client_id_fkey already exists, not creating it.';
    END;

    BEGIN
      ALTER TABLE holds ADD COLUMN external_identifier varchar UNIQUE;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column holds.external_identifier already exists, not creating it.';
    END;
  END
$$;
