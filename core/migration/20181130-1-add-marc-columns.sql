DO $$
  BEGIN
    ALTER TABLE works ADD COLUMN marc_record varchar;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'marc_record column already exists, not creating it.';
  END;
$$;
