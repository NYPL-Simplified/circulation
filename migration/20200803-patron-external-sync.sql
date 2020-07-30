DO $$ 
 BEGIN
  -- Add the 'last_loan_activity_sync' column
  BEGIN
   ALTER TABLE patrons ADD COLUMN last_loan_activity_sync datetime;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column patrons.last_loan_activity_sync already exists, not creating it.';
  END;
 END;
$$;
