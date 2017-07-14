alter table externalintegrations add column name character varying;
create index "ix_externalintegrations_name" on externalintegrations (name);

alter table externalintegrations add column protocol varchar;
create index "ix_externalintegrations_protocol" on externalintegrations (protocol);

alter table externalintegrations add column goal varchar;
create index "ix_externalintegrations_goal" on externalintegrations (goal);
