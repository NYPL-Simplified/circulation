-- genres.name should be indexed and unique.
DO $$ 
    BEGIN
        BEGIN
create unique index ix_genres_name on genres (name);
        EXCEPTION
            WHEN OTHERS THEN RAISE NOTICE 'WARNING: it looks like ix_genres_name already exists; it was probably created on initial database creation.';
        END;
    END;
$$;
