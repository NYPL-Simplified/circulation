ALTER TABLE licensepools ADD COLUMN presentation_edition_id integer;
CREATE INDEX ix_licensepools_presentation_edition_id ON licensepools USING btree (presentation_edition_id);
ALTER TABLE ONLY licensepools ADD CONSTRAINT licensepools_presentation_edition_id_fkey FOREIGN KEY (presentation_edition_id) REFERENCES editions(id);
update licensepools set presentation_edition_id = (select id from editions e where licensepools.data_source_id=e.data_source_id and licensepools.identifier_id=e.primary_identifier_id);
