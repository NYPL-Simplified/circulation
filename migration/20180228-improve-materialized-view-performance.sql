DO $$
    BEGIN
        -- Create indices for foreign keys on materialized views.
        BEGIN
            create index ix_mv_works_for_lanes_works_id on
                mv_works_for_lanes (works_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_mv_works_for_lanes_works_id already exists.';
        END;

        BEGIN
            create index ix_mv_works_for_lanes_license_pool_id on
                mv_works_for_lanes (license_pool_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_mv_works_for_lanes_license_pool_id already exists.';
        END;

        BEGIN
            create index ix_mv_works_for_lanes_workgenres_id on
                mv_works_for_lanes (workgenres_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_mv_works_for_lanes_workgenres_id already exists.';
        END;

        BEGIN
            create index ix_mv_works_for_lanes_list_id on
                mv_works_for_lanes (list_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_mv_works_for_lanes_list_id already exists.';
        END;

        BEGIN
            create index ix_mv_works_for_lanes_list_edition_id on
                mv_works_for_lanes (list_edition_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_mv_works_for_lanes_list_edition_id already exists.';
        END;

        BEGIN
            create index ix_licensepools_collection_id on
                licensepools (collection_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_licensepools_collection_id already exists.';
        END;

        BEGIN
            create index ix_licensepools_licenses_owned on
                licensepools (licenses_owned);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_licensepools_licenses_owned already exists.';
        END;
    END;
$$;
