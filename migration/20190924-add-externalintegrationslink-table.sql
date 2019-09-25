DO $$
    BEGIN
        BEGIN
            CREATE TABLE externalintegrationslinks (
                id SERIAL PRIMARY KEY,
                external_integration_id INTEGER REFERENCES externalintegrations(id),
                library_id INTEGER REFERENCES libraries(id),
                other_integration_id INTEGER REFERENCES externalintegrations(id),
                purpose VARCHAR
            );
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: externalintegrationslinks already exists.';
        END;
    END;
$$;