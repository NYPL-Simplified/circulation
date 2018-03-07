DO $$
    BEGIN
        -- Delete the index if it already exists.
        BEGIN
            drop index mv_works_for_lanes_by_recently_updated;
        EXCEPTION
            WHEN OTHERS THEN RAISE NOTICE 'mv_works_for_lanes_by_recently_updated did not previously exist.';
        END;
	create index mv_works_for_lanes_by_recently_updated on mv_works_for_lanes (GREATEST(availability_time, first_appearance, last_update_time) DESC, collection_id, works_id);

        BEGIN
	    create index mv_works_for_lanes_list_and_collection_id on mv_works_for_lanes (list_id, collection_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: mv_works_for_lanes_list_and_collection_id already exists.';
        END;
    END
$$;
