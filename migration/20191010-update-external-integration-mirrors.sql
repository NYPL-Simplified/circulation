-- Previously, collections could only have one mirror integration associated
-- with it. Now, a collection can currently have two external integration storages
-- for its "books" and "covers" mirrors. Any existing mirror integration
-- associated with a collection is now linked through the ExternalIntegrationsLinks
-- table. The mirror integration will be set to both "books" and "covers" mirrors.


insert into externalintegrationslinks (external_integration_id, other_integration_id, purpose)
select external_integration_id, mirror_integration_id, 'books'
from collections
where mirror_integration_id is not null;

insert into externalintegrationslinks (external_integration_id, other_integration_id, purpose)
select external_integration_id, mirror_integration_id, 'covers'
from collections
where mirror_integration_id is not null;

ALTER TABLE collections DROP COLUMN mirror_integration_id;