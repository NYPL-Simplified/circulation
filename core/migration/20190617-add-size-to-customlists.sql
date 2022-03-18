DO $$ 
    BEGIN
        BEGIN
            ALTER TABLE customlists ADD COLUMN size integer not null default 0;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column customlists.size already exists, not creating it.';
        END;
        UPDATE customlists SET size = COALESCE(
            (SELECT subq.c FROM
                (SELECT list_id AS l, count(*) AS c FROM customlistentries GROUP BY list_id)
                AS subq WHERE subq.l = customlists.id),
            0);
    END $$;
