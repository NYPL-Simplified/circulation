ALTER TABLE loans ADD COLUMN integration_client_id integer;
ALTER TABLE loans ADD CONSTRAINT loans_integration_client_id_fkey foreign key (integration_client_id) references integrationclients(id);

ALTER TABLE holds ADD COLUMN integration_client_id integer;
ALTER TABLE holds ADD CONSTRAINT holds_integration_client_id_fkey foreign key (integration_client_id) references integrationclients(id);

ALTER TABLE holds ADD COLUMN external_identifier varchar UNIQUE;
