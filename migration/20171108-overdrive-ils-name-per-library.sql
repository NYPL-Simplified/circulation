-- Find every Overdrive integration that defines an 'ils_name' key,
-- and copy that key to every library that uses the corresponding
-- Overdrive collection.
insert into configurationsettings (external_integration_id, library_id, key, value) 
       select distinct ei.id, cl.library_id, key, value
       from configurationsettings cs
       join externalintegrations ei on cs.external_integration_id=ei.id
       join collections c on c.external_integration_id=ei.id
       join collections_libraries cl on c.id=cl.collection_id
       where ei.protocol='Overdrive'
       and cs.key='ils_name'
       and not exists (select * from configurationsettings where library_id = cl.library_id and external_integration_id = ei.id and key = 'ils_name');

-- Delete all 'ils_name' configuration settings associated with an
-- Overdrive integration but not affiliated with any library.
delete from configurationsettings where library_id is null and key='ils_name' and external_integration_id in (select id from externalintegrations where protocol='Overdrive');
