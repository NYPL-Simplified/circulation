-- Remove the 'generate-opds` coverage record for all works.
delete from workcoveragerecords where operation='generate-opds';
