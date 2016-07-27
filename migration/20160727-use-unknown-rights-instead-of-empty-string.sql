UPDATE licensepooldeliveries
  FROM licensepools AS lp
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  WHERE lp.data_source_id = (SELECT id from datasources where name = 'Overdrive');

UPDATE licensepooldeliveries
  FROM licensepools AS lp
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  WHERE lp.data_source_id = (SELECT id from datasources where name = '3M');

UPDATE licensepooldeliveries
  FROM licensepools AS lp
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/in-copyright')
  WHERE lp.data_source_id = (SELECT id from datasources where name = 'Axis 360');

UPDATE licensepooldeliveries
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/unknown')
  WHERE rightsstatus_id is null;

UPDATE licensepooldeliveries 
  SET rightsstatus_id = (SELECT id from rightsstatus WHERE uri = 'http://librarysimplified.org/terms/rights-status/unknown')
  WHERE rightsstatus_id = (SELECT id from rightsstatus WHERE uri is null);