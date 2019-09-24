DO $$
    BEGIN
        BEGIN
            CREATE TABLE mirrors (
                id SERIAL PRIMARY KEY,
                external_integration_id INTEGER REFERENCES externalintegrations(id),
                library_id INTEGER REFERENCES libraries(id),
                mirror_integration_id INTEGER REFERENCES externalintegrations(id),
                purpose VARCHAR
            );
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: mirrors already exists.';
        END;
    END;
$$;