 BEGIN
  -- Add the 'cached_manifest' column
  BEGIN
   ALTER TABLE loans ADD COLUMN cached_manifest bytea;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column loans.cached_manifest already exists, not creating it.';
  END;

  -- Add the 'cached_content_type' column'
  BEGIN
   ALTER TABLE loans ADD COLUMN cached_content_type varchar;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column loans.cached_content_type already exists, not creating it.';
  END;

 END;
$$;
