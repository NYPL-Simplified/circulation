alter table integrationclients drop column key;
alter table integrationclients drop column _secret;
alter table integrationclients add column shared_secret varchar;
