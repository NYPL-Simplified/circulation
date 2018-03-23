-- Add collections.mirror_integration_id, a foreign key
-- against externalintegrations.id
alter table collections add column mirror_integration_id integer;
alter table collections add constraint collections_mirror_integration_id_fkey FOREIGN KEY (mirror_integration_id) REFERENCES externalintegrations(id);
