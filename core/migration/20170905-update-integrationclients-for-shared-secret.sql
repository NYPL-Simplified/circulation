-- Reset the integrationclients table.
alter table integrationclients drop column _secret;
alter table integrationclients rename column key to shared_secret;
drop index if exists ix_integrationclients_key;
delete from integrationclients;
