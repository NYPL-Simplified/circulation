-- Moving the "Library ID" Enki integration setting to be associated with each library
-- and not just the collection.
insert into configurationsettings (external_integration_id, library_id, key, value)
select externalintegration_id, library_id, 'enki_library_id', external_account_id
from collections join externalintegrations_libraries as el
on collections.external_integration_id=el.externalintegration_id
join externalintegrations as e on e.id=el.externalintegration_id where e.protocol='Enki';


-- Remove external_account_id values for all Enki collections
update collections
set external_account_id=null
where external_integration_id in (select id from externalintegrations where protocol='Enki');
