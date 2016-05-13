update licensepools set open_access=false where open_access is null and data_source_id=(select id from datasources where name='3M');
