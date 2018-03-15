DO $$
    BEGIN
        -- Delete the index if it already exists.
        BEGIN
            drop index mv_works_for_lanes_by_recently_updated;
        EXCEPTION
            WHEN OTHERS THEN RAISE NOTICE 'mv_works_for_lanes_by_recently_updated did not previously exist.';
        END;
	create index mv_works_for_lanes_by_recently_updated on mv_works_for_lanes (GREATEST(availability_time, first_appearance, last_update_time) DESC, collection_id, works_id);
    END
$$;
