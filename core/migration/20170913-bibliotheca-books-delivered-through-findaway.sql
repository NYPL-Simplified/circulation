-- Create a new delivery mechanism for audiobooks delivered
-- through Findaway
insert into deliverymechanisms (content_type, drm_scheme) values (
       'audio/mpeg',
       'application/vnd.librarysimplified.findaway.license+json'
);

-- Update all audiobooks delivered through Bibliotheca to use
-- the new delivery mechanism.
update licensepooldeliveries set delivery_mechanism_id = (
 select id from deliverymechanisms where 
  content_type='audio/mpeg' and 
  drm_scheme='application/vnd.librarysimplified.findaway.license+json'
) 

where id in (
 select lpd.id from licensepooldeliveries lpd
  join deliverymechanisms dm on lpd.delivery_mechanism_id=dm.id
  join datasources ds on lpd.data_source_id=ds.id 
  where ds.name='Bibliotheca'
  and dm.content_type='audio/mpeg'
);
