-- We no longer make any assumptions about the medium associated
-- with an edition.
ALTER TABLE editions ALTER COLUMN medium DROP DEFAULT;
