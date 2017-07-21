-- Remove the existing indices from coveragerecords
drop index if exists ix_coveragerecords_data_source_id_operation_identifier_id;
alter table coveragerecords drop constraint if exists coveragerecords_identifier_id_data_source_id_operation_key;

-- Add collection_id as a foreign key
alter table coveragerecords add column collection_id integer;
alter table coveragerecords add constraint coveragerecords_collection_id_fkey
    foreign key (collection_id) references collections(id);

-- Create unique indices for coveragerecords with or without a collection_id
create unique index ix_identifier_id_data_source_id_operation
    on coveragerecords (identifier_id, data_source_id, operation)
    where collection_id is null;

create unique index ix_identifier_id_data_source_id_operation_collection_id
    on coveragerecords (identifier_id, data_source_id, operation, collection_id);
