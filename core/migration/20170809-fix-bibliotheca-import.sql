update identifiers
  set type = 'Bibliotheca ID'
  where type in ('3M ID', 'Bibliotecha ID');
  
update editions
  set data_source_id = (select id from datasources where name = 'Bibliotheca')
  where data_source_id in (select id from datasources where name in ('3M', 'Bibliotecha'));
  
update coveragerecords
  set data_source_id = (select id from datasources where name = 'Bibliotheca')
  where data_source_id in (select id from datasources where name in ('3M', 'Bibliotecha'));
