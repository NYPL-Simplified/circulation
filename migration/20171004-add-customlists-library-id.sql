DO $$ 
    BEGIN
        BEGIN
            ALTER TABLE customlists ADD COLUMN library_id integer;
	    alter table customlists
	        add constraint customlists_library_id_fkey
		foreign key (library_id)
		references libraries(id);

	    create index "ix_customlists_library_id" ON customlists (library_id);

	    alter table customlists drop constraint if exists "customlists_data_source_id_name_key";
	    alter table customlists add constraint "customlists_data_source_id_name_library_id_key" unique (data_source_id, name, library_id);

        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'WARNING: column library_id already exists in customlists. Verify that 20171004-add-customlists-library-id.sql is redundant.';
        END;
    END;
$$;


