DO $$
 BEGIN
  -- Add the 'cached_neighborhood' column
  BEGIN
   ALTER TABLE patrons ADD COLUMN cached_neighborhood varchar;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column patrons.cached_neighborhood already exists, not creating it.';
  END;

  -- Index this field so we can easily scrub values when the cache expires.
  BEGIN
   CREATE index ix_patrons_cached_neighborhood on patrons (cached_neighborhood);
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'index ix_patrons_cached_neighborhood already exists; leaving it alone.';
  END;

 END;
$$;
