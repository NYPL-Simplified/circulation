DO $$ 
    BEGIN
        BEGIN
            ALTER TABLE lanes ADD COLUMN size integer not null default 0;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column lanes.size already exists, not creating it.';
        END;
    END $$;

