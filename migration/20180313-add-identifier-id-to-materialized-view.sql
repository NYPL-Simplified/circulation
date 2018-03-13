DO $$
    BEGIN
        BEGIN
	    create index mv_works_for_lanes_identifier_id on mv_works_for_lanes (identifier_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: mv_works_for_lanes_identifier_id already exists.';
        END;
    END
$$;
