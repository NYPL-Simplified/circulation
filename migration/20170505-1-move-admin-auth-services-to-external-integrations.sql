UPDATE externalintegrations ei
SET type = 'admin_auth', provider = aas.provider
FROM adminauthenticationservices as aas
WHERE ei.id = aas.external_integration_id;

DROP TABLE adminauthenticationservices;
