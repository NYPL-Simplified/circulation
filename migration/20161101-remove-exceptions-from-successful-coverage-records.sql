update coveragerecords set exception = null where status = 'success' and exception is not null;
update workcoveragerecords set exception = null where status = 'success' and exception is not null;
