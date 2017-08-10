DO $$
DECLARE registry_id int;
DECLARE sct_id int;

BEGIN
        -- Create a new registry integration, and store its id.
        INSERT INTO externalintegrations (goal, protocol, name)
        VALUES ('discovery', 'OPDS Registration', 'Library Simplified Registry')
        RETURNING id into registry_id;

        -- Create the registry's url setting.
        INSERT INTO configurationsettings (key, value, external_integration_id)
        VALUES ('url', 'https://registry.librarysimplified.org', registry_id);

        -- Find the short client token integration and store its id.
        SELECT id INTO sct_id FROM externalintegrations
        WHERE protocol='Short Client Token' AND GOAL='drm' LIMIT 1;

        -- Move the vendor id from the short client token to the registry.
        UPDATE configurationsettings SET external_integration_id=registry_id
        WHERE key='vendor_id' and external_integration_id=sct_id;

        -- Move usernames from the short client token to the registry.
        UPDATE configurationsettings SET external_integration_id=registry_id
        WHERE key='username' and external_integration_id=sct_id;

        -- Move passwords from the short client token to the registry.
        UPDATE configurationsettings SET external_integration_id=registry_id
        WHERE key='password' and external_integration_id=sct_id;

        -- Move libraries from the short client token to the registry.
        UPDATE externalintegrations_libraries SET externalintegration_id=registry_id
        WHERE externalintegration_id=sct_id;

        -- Drop the short client token integration.
        DELETE FROM externalintegrations WHERE id=sct_id;

END $$;