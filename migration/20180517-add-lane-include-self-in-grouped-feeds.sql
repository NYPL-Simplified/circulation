DO $$
  BEGIN
    BEGIN
      ALTER TABLE lanes ADD COLUMN include_self_in_grouped_feed boolean default true not null;
    EXCEPTION
      WHEN duplicate_column THEN RAISE NOTICE 'column lanes.include_self_in_grouped_feed already exists, not creating it.';
    END;
  END;
$$;
