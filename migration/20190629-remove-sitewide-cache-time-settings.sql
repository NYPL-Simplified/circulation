-- These site-wide configuration settings are no longer used.
delete from configurationsettings where library_id is null and external_integration_id is null and key in ('default_nongrouped_feed_max_age', 'default_grouped_feed_max_age');
