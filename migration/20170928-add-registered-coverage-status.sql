ALTER TYPE coverage_status ADD VALUE IF NOT EXISTS 'registered' AFTER 'persistent failure';

update coveragerecords
  set
    status = 'registered',
    exception = null
  where
    status = 'transient failure' and exception like 'No work done yet%';

