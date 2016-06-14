CREATE TYPE coverage_status AS ENUM (
    'success',
    'transient failure',
    'persistent failure'
);

alter table coveragerecords add column status coverage_status;
alter table workcoveragerecords add column status coverage_status;

CREATE INDEX ix_coveragerecords_status ON coveragerecords USING btree (status);
CREATE INDEX ix_workcoveragerecords_status ON workcoveragerecords USING btree (status);

update table coveragerecords set status='success' where exception is null;
update table coveragerecords set status='persistent failure' where exception is not null;

update table workcoveragerecords set status='success' where exception is null;
update table workcoveragerecords set status='persistent failure' where exception is not null;
