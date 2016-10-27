alter table patrons add column fines2 numeric;
update patrons set fines2=to_number(substr(fines, 2, 100), '9999.99');
alter table patrons drop column fines;
alter table patrons rename column fines2 to fines;
