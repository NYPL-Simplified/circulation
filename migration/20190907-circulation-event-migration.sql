-- Delete all duplicate circulation events before creating unique indexes.
delete from circulationevents where id in (select ce1.id from circulationevents ce1 join circulationevents as ce2 on ce1.license_pool_id = ce2.license_pool_id and ce1.type = ce2.type and ce1.start = ce2.start and ce1.id < ce2.id);

DO $$ 
 BEGIN
  -- Add the 'location' column
  BEGIN
   ALTER TABLE circulationevents ADD COLUMN location varchar;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column circulationevents.location already exists, not creating it.';
  END;

  -- Add the 'library_id' column
  BEGIN
   ALTER TABLE circulationevents ADD COLUMN library_id int;
  EXCEPTION
   WHEN duplicate_column THEN RAISE NOTICE 'column circulationevents.library_id already exists, not creating it.';
  END;

  -- Make the 'library_id' column a foreign key reference to libraries.id.
  BEGIN
   ALTER TABLE circulationevents ADD CONSTRAINT circulationevents_library_id_fkey FOREIGN KEY (library_id) REFERENCES libraries(id);
  EXCEPTION
   when duplicate_object THEN RAISE NOTICE 'column circulationevents.library_id is already a foreign key; leaving it alone.';
  END;

  -- Remove the unique constraint that involves the 'foreign_patron_id' field.
  alter table circulationevents drop constraint if exists circulationevents_license_pool_id_type_start_foreign_patron_key;

  -- Drop the 'foreign_patron_id' field itself.
  alter table circulationevents drop column if exists foreign_patron_id;

  -- Create some indexes to enforce uniqueness constraints.

  -- If there is no library ID, then licence pool + type + start
  -- must be unique.
  BEGIN
   CREATE UNIQUE index ix_circulationevents_license_pool_type_start on circulationevents (license_pool_id, type, start) where library_id is null;
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'unique index ix_circulationevents_license_pool_type_start already exists; leaving it alone.';
  END;

  -- If there is a library ID, then license pool + library +
  -- type + start must be unique.
  BEGIN
   CREATE UNIQUE index ix_circulationevents_license_pool_library_type_start on circulationevents (license_pool_id, library_id, type, start);
  EXCEPTION
   WHEN duplicate_table THEN RAISE NOTICE 'unique index ix_circulationevents_license_pool_library_type_start already exists; leaving it alone.';
  END;

 END;
$$;
