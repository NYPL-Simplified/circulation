ALTER TABLE coveragerecords RENAME date TO timestamp;
ALTER TABLE coveragerecords ALTER COLUMN timestamp SET DATA TYPE timestamp;
ALTER TABLE coveragerecords ADD COLUMN operation varchar(255) DEFAULT NULL;
CREATE UNIQUE INDEX "ix_coveragerecords_data_source_id_operation_identifier_id" ON coveragerecords (data_source_id, operation, identifier_id);
ALTER TABLE ONLY coveragerecords ADD CONSTRAINT coveragerecords_identifier_id_data_source_id_operation_key UNIQUE (identifier_id, data_source_id, operation);
