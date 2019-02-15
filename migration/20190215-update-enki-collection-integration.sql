-- Moving the "Library ID" Enki integration setting to be associated with each library
-- and not just the collection.
update configurationsettings as cs
set
    external_integration_id=updatesetting.externalintegration_id,
    library_id=updatesetting.library_id,
    key='external_account_id',
    value=updatesetting.external_account_id
from (
    select externalintegration_id, library_id, external_account_id
    from collections join externalintegrations_libraries as el
    on collections.external_integration_id=el.externalintegration_id
    join externalintegrations as e on e.id=el.externalintegration_id where e.protocol='Enki'
) updatesetting
where
    cs.external_integration_id=updatesetting.externalintegration_id
    and cs.library_id=updatesetting.library_id
    and key='external_account_id';