-- Associate all 'Staff Picks' customlists with the default library,
-- unless they're already associated with some other library.
update customlists set library_id = (
 select id from libraries where is_default=true limit 1
) where library_id is null and foreign_identifier='Staff Picks';
