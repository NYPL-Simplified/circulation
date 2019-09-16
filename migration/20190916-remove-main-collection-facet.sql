-- 'main' is no longer a recognized facet in the 'collection' facet
-- group.

-- This migration script removes it from the list of enabled facets,
-- if it's present, and changes the default if 'main' is the default.

-- Remove '"main",' for cases where "main" is not the last item in the JSON list.
update configurationsettings set value = replace(value, '"main",', '') where key='facets_enabled_collection';

-- Remove ', "main"' for cases where "main" is the last item in the JSON list.
update configurationsettings set value = replace(value, ', "main"]', ']') where key='facets_enabled_collection';

-- If 'main' is the *only* enabled collection facet, use 'full' instead.
update configurationsettings set value = '["full"]' where key='facets_enabled_collection' and value='["main"]';

-- If 'main' was the default collection, change it.
update configurationsettings set value='full' where key='facets_default_collection' and value='main';
