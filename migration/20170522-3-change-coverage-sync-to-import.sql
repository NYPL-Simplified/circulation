UPDATE coveragerecords
SET operation = 'import'
WHERE
    data_source_id in (
        select id from datasources where name = 'Library Simplified metadata wrangler'
    ) and operation = 'sync';
