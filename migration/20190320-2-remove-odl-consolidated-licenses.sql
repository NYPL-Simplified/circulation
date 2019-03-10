-- Temporarily wipe out all the licenses in ODL collections and delete their coverage records. Another migration will run a full import and add individual licenses.

update licensepools set licenses_owned = 0 from collections join externalintegrations on collections.external_integration_id = externalintegrations.id where licensepools.collection_id = collections.id and externalintegrations.protocol = 'ODL with Consolidated Copies';
update licensepools set licenses_available = 0 from collections join externalintegrations on collections.external_integration_id = externalintegrations.id where licensepools.collection_id = collections.id and externalintegrations.protocol = 'ODL with Consolidated Copies';

delete from coveragerecords where coveragerecords.identifier_id in (select identifiers.id from identifiers join licensepools on licensepools.identifier_id = identifiers.id join collections on licensepools.collection_id = collections.id join externalintegrations on collections.external_integration_id = externalintegrations.id where externalintegrations.protocol = 'ODL With Consolidated Copies') and coveragerecords.operation = 'import';

update externalintegrations set protocol = 'ODL' where protocol = 'ODL with Consolidated Copies';