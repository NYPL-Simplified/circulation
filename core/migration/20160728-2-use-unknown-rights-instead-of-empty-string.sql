UPDATE licensepooldeliveries
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  FROM licensepools AS lp
  WHERE lp.data_source_id = (SELECT id from datasources where name = 'Overdrive')
  AND lp.id = licensepooldeliveries.license_pool_id;

UPDATE licensepooldeliveries
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  FROM licensepools AS lp
  WHERE lp.data_source_id = (SELECT id from datasources where name = '3M')
  AND lp.id = licensepooldeliveries.license_pool_id;

UPDATE licensepooldeliveries
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  FROM licensepools AS lp
  WHERE lp.data_source_id = (SELECT id from datasources where name = 'Axis 360')
  AND lp.id = licensepooldeliveries.license_pool_id;

UPDATE licensepooldeliveries
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/unknown')
  WHERE rightsstatus_id is null;

UPDATE licensepooldeliveries 
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/unknown')
  WHERE rightsstatus_id = (SELECT id from rightsstatus WHERE uri is null);