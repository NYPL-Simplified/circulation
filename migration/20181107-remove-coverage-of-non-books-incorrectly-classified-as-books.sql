-- Delete all choose-edition WorkCoverageRecords for works whose
-- presentation edition says they are books, but whose data sources say
-- they are not books.
--
-- Later, WorkPresentationEditionCoverageProvider will run and recalculate
-- the presentation editions properly.

-- We're going to delete some rows from workcoveragerecords.
delete from workcoveragerecords where operation='choose-edition' and
  work_id in (
     -- We're looking for works whose presentation editions say they are books.
     select w.id
      from works w
           join licensepools lp on lp.work_id=w.id
           join editions e on lp.presentation_edition_id=e.id
      where
        e.medium='Book'
        and lp.id in (
          -- We're looking for works associated with license pools
          -- with an associated identifier that says they are _not_ books.
          select lp.id
            from licensepools lp
                 join editions e on (
                   lp.data_source_id=e.data_source_id
		   and lp.identifier_id=e.primary_identifier_id
                 )
	     where e.medium != 'Book'
          )
    )
;
