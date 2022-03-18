alter table loans add column license_id integer;
alter table loans add constraint loans_license_id_key foreign key (license_id) references licenses(id);
