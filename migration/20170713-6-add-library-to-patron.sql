-- It used to be that patrons.username,
-- patrons.authorization_identifier, and patrons.external_identifier
-- each had to be unique. Now, the *combination* of each of those fields
-- with patrons.library_id must be unique.

DROP INDEX IF EXISTS ix_patrons_authorization_identifier;
DROP INDEX IF EXISTS ix_patrons_external_identifier;
DROP INDEX IF EXISTS ix_patrons_username;

ALTER TABLE patrons ADD COLUMN library_id integer;
ALTER TABLE patrons ADD CONSTRAINT patrons_library_id_fkey FOREIGN KEY (library_id) REFERENCES libraries(id);


ALTER TABLE patrons ADD CONSTRAINT patrons_library_id_authorization_identifier_key UNIQUE (library_id, authorization_identifier);
ALTER TABLE patrons ADD CONSTRAINT patrons_library_id_external_identifier_key UNIQUE (library_id, external_identifier);
ALTER TABLE patrons ADD CONSTRAINT patrons_library_id_username_key UNIQUE (library_id, username);

CREATE INDEX ix_patron_library_id_authorization_identifier ON patrons USING btree (library_id, authorization_identifier);
CREATE INDEX ix_patron_library_id_external_identifier ON patrons USING btree (library_id, external_identifier);
CREATE INDEX ix_patron_library_id_username ON patrons USING btree (library_id, username);

UPDATE patrons set library_id = (select id from libraries limit 1);
