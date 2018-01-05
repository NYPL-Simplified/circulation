-- The default for lanes.inherit_parent_restrictions has changed
-- to true. Change the setting for all lanes not based on custom lists.
-- For all lanes based on custom lists, a human has made a decision
-- to set the value one way or the other.
update lanes set inherit_parent_restrictions=true where _list_datasource_id is null and id not in (select lane_id from lanes_customlists);
ALTER TABLE lanes ALTER COLUMN inherit_parent_restrictions SET DEFAULT true;
