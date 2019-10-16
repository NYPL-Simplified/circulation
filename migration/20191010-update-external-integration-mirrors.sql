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

            CREATE INDEX ix_externalintegrationslinks_external_integration_id ON externalintegrationslinks USING btree (external_integration_id);
            CREATE INDEX ix_externalintegrationslinks_library_id ON externalintegrationslinks USING btree (library_id);
            CREATE INDEX ix_externalintegrationslinks_other_integration_id ON externalintegrationslinks USING btree (other_integration_id);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Warning: externalintegrationslinks already exists.';
        END;
    END;
$$;

-- Previously, collections could only have one mirror integration associated
-- with it. Now, a collection can currently have two external integration storages
-- for its "books_mirror" and "covers_mirror" mirrors. Any existing mirror integration
-- associated with a collection is now linked through the ExternalIntegrationsLinks
-- table. The mirror integration will be set to both "books_mirror" and "covers_mirror" mirrors.


insert into externalintegrationslinks (external_integration_id, other_integration_id, purpose)
select external_integration_id, mirror_integration_id, 'books_mirror'
from collections
where mirror_integration_id is not null;

insert into externalintegrationslinks (external_integration_id, other_integration_id, purpose)
select external_integration_id, mirror_integration_id, 'covers_mirror'
from collections
where mirror_integration_id is not null;

ALTER TABLE collections DROP COLUMN mirror_integration_id;