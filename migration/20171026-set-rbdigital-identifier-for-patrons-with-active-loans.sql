-- Set patron's authorization identifier as their RBdigital identifier
insert into credentials(data_source_id, patron_id, type, credential) select
 datasources.id,
 patrons.id,
 'Identifier Sent To Remote Service',
 patrons.authorization_identifier
from patrons join datasources on datasources.name='RBdigital'

-- If they don't already have a credential
where patrons.id not in (
 select patron_id from credentials join datasources on credentials.data_source_id=datasources.id and datasources.name='RBdigital' where type='Identifier Sent To Remote Service'
) and (
 -- And they have an active RBdigital loan or hold.
 patrons.id in (
  select patron_id from loans join licensepools on loans.license_pool_id=licensepools.id join datasources on licensepools.data_source_id=datasources.id and datasources.name='RBdigital'
 ) or patrons.id in (
 select patron_id from holds join licensepools on holds.license_pool_id=licensepools.id join datasources on licensepools.data_source_id=datasources.id and datasources.name='RBdigital'
 )
)
;
