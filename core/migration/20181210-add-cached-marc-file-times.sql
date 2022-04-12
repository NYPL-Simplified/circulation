DO $$
  BEGIN
    BEGIN
      ALTER TABLE cachedmarcfiles ADD COLUMN start_time timestamp;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'start_time column already exists, not creating it.';
    END;
    BEGIN
      ALTER TABLE cachedmarcfiles ADD COLUMN end_time timestamp;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'end_time column already exists, not creating it.';
    END;
  END;
$$;