update externalintegrations
    set goal = 'licenses'
    where id in (
        select ei.id from externalintegrations ei
        join collections c on ei.id = c.external_integration_id
    );
 
