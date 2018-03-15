DO $$
    BEGIN
        BEGIN
	    drop index mv_works_for_lanes_list_and_collection_id;
        EXCEPTION
            WHEN OTHERS THEN RAISE NOTICE 'mv_works_for_lanes_list_and_collection_id is already gone.';
        END;

	BEGIN
	    create index mv_works_for_lanes_list_id_collection_id_language_medium on mv_works_for_lanes (list_id, collection_id, language, medium);
	EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: mv_works_for_lanes_list_id_collection_id_language_medium already exists.';
        END;

	BEGIN
	    create index customlistentries_work_id_list_id on customlistentries (work_id, list_id);
	EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: customlistentries_work_id_list_id already exists.';
        END;
    END
$$;

