 DO $$
    BEGIN
	BEGIN
	    create index ix_loans_start on loans (start);
	EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: is_loans_start already exists.';
        END;

	BEGIN
	    create index ix_loans_end on loans ("end");
	EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_loans_end already exists.';
        END;

	BEGIN
	    create index ix_cachedfeeds_timestamp on cachedfeeds (timestamp);
	EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_cachedfeeds_timestamp already exists.';
        END;
    END;
$$;
