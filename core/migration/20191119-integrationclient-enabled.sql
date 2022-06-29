DO $$
 BEGIN
  -- Add the 'enabled' column
  BEGIN
   ALTER TABLE integrationclients ADD COLUMN enabled boolean default true;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column integrationclients.enabled already exists, not creating it.';
  END;
 END;
$$;
