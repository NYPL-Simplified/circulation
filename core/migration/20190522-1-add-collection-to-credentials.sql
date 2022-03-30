alter table credentials add column collection_id integer;
alter table credentials add constraint "credentials_collection_id_fkey" FOREIGN KEY (collection_id) REFERENCES collections(id);
alter table credentials add constraint "credentials_data_source_id_patron_id_collection_id_type_key" UNIQUE (data_source_id, patron_id, collection_id, type);
alter table credentials drop constraint credentials_data_source_id_patron_id_type_key;
create index ix_credentials_collection_id on credentials(collection_id);