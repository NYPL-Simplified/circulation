ALTER TABLE loans DROP CONSTRAINT loans_patron_id_fkey;
ALTER TABLE loans ADD CONSTRAINT loans_patron_id_fkey FOREIGN KEY (patron_id) REFERENCES patrons (id) ON DELETE CASCADE;

ALTER TABLE holds DROP CONSTRAINT holds_patron_id_fkey;
ALTER TABLE holds ADD CONSTRAINT holds_patron_id_fkey FOREIGN KEY (patron_id) REFERENCES patrons (id) ON DELETE CASCADE;

ALTER TABLE annotations DROP CONSTRAINT annotations_patron_id_fkey;
ALTER TABLE annotations ADD CONSTRAINT annotations_patron_id_fkey FOREIGN KEY (patron_id) REFERENCES patrons (id) ON DELETE CASCADE;

ALTER TABLE credentials DROP CONSTRAINT credentials_patron_id_fkey;
ALTER TABLE credentials ADD CONSTRAINT credentials_patron_id_fkey FOREIGN KEY (patron_id) REFERENCES patrons (id) ON DELETE CASCADE;
