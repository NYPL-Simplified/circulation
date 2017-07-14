-- Create two new columns that will replace license_pool_id.
ALTER TABLE licensepooldeliveries ADD COLUMN data_source_id integer;
ALTER TABLE licensepooldeliveries ADD COLUMN identifier_id integer;

alter table licensepooldeliveries
    add constraint licensepooldeliveries_data_source_id_fkey
    foreign key (data_source_id)
    references datasources(id);

alter table licensepooldeliveries
    add constraint licensepooldeliveries_identifier_id_fkey
    foreign key (identifier_id)
    references identifiers(id);

-- Copy in appropriate values using license_pool_id.
update licensepooldeliveries set
 data_source_id=subquery.data_source_id,
 identifier_id=subquery.identifier_id from (
    select lpdm.id as delivery_id,
    	   lp.identifier_id as identifier_id,
	   lp.data_source_id as data_source_id
    from licensepooldeliveries lpdm
    	 join licensepools lp on lpdm.license_pool_id=lp.id
) as subquery where licensepooldeliveries.id=subquery.delivery_id;

-- Now that we have the data, create a unique index.
CREATE UNIQUE INDEX ix_licensepooldeliveries_datasource_identifier_mechanism on licensepooldeliveries USING btree (data_source_id, identifier_id, delivery_mechanism_id, resource_id);

-- Finally, remove the now-unnecessary license_pool_id.
ALTER TABLE licensepooldeliveries DROP COLUMN license_pool_id;
