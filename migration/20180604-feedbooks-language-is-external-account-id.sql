-- For each FeedBooks language, copy the value of the 'language' setting
-- to the collection's external_account_id.
update collections set external_account_id='en' where id in (
 select c.id from collections c join externalintegrations e on c.external_integration_id=e.id join configurationsettings cs on cs.external_integration_id=e.id where e.protocol='FeedBooks' and cs.key='language' and cs.value='en'
);
update collections set external_account_id='fr' where id in (
 select c.id from collections c join externalintegrations e on c.external_integration_id=e.id join configurationsettings cs on cs.external_integration_id=e.id where e.protocol='FeedBooks' and cs.key='language' and cs.value='fr'
);
update collections set external_account_id='de' where id in (
 select c.id from collections c join externalintegrations e on c.external_integration_id=e.id join configurationsettings cs on cs.external_integration_id=e.id where e.protocol='FeedBooks' and cs.key='language' and cs.value='de'
);
update collections set external_account_id='it' where id in (
 select c.id from collections c join externalintegrations e on c.external_integration_id=e.id join configurationsettings cs on cs.external_integration_id=e.id where e.protocol='FeedBooks' and cs.key='language' and cs.value='it'
);
update collections set external_account_id='es' where id in (
 select c.id from collections c join externalintegrations e on c.external_integration_id=e.id join configurationsettings cs on cs.external_integration_id=e.id where e.protocol='FeedBooks' and cs.key='language' and cs.value='es'
);

-- Delete all FeedBooks language settings.
delete from configurationsettings where id in (
 select cs.id from configurationsettings cs join externalintegrations e on cs.external_integration_id=e.id where cs.key='language' and e.protocol='FeedBooks'
);

