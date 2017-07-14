alter table externalintegrations add column name character varying;
create index "ix_externalintegrations_name" on externalintegrations (name);
