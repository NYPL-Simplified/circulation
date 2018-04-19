DO $$
    BEGIN
        BEGIN
            CREATE TABLE adminroles (
                id SERIAL PRIMARY KEY,
                admin_id INTEGER NOT NULL REFERENCES admins(id),
                library_id INTEGER REFERENCES libraries(id),
                role VARCHAR NOT NULL
            );
            ALTER TABLE adminroles ADD CONSTRAINT adminroles_admin_id_library_id_role UNIQUE (admin_id, library_id, role);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: adminroles already exists.';
        END;

        BEGIN
            CREATE UNIQUE INDEX ix_adminroles_admin_id_library_id_role
                ON adminroles (admin_id, library_id, role);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: ix_adminroles_admin_id_library_id_role already exists.';
        END;

        INSERT INTO adminroles (admin_id, role) (SELECT id, 'system' from admins);
    END;
$$;