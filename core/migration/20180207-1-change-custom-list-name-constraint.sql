alter table customlists drop constraint if exists "customlists_data_source_id_name_library_id_key";
alter table customlists add constraint "customlists_name_library_id_key" unique (name, library_id);