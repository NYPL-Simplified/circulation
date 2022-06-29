-- Remove the 'generate-opds` coverage record for any works
-- that have an associated series.
delete from workcoveragerecords where operation='generate-opds' and work_id in (
       select w.id from works w join editions e on w.presentation_edition_id=e.id where e.series is not null
);

-- Remove the 'generate-opds` coverage record for any works
-- that have contributors in non-author roles.
delete from workcoveragerecords where operation='generate-opds' and work_id in (
       select distinct w.id from works w join editions e on w.presentation_edition_id=e.id join contributions c on e.id=c.edition_id where c.role not in ('Author', 'Primary Author')
);

-- These records will be added back when the OPDSEntryWorkCoverageProvider
-- regenerates their OPDS entries.
