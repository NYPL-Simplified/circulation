DO $$ 
 BEGIN
  -- Add the 'self_hosted' column
  BEGIN
   ALTER TABLE licensepools ADD COLUMN self_hosted BOOLEAN DEFAULT false;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column licensepools.self_hosted already exists, not creating it.';
  END;
 END;
$$;

