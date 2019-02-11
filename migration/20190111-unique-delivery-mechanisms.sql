-- The ix_licensepooldeliveries_datasource_identifier_mechanism index
-- is not necessary - an index is automatically created on the same
-- fields to enforce a uniqueness constraint.
drop index if exists ix_licensepooldeliveries_datasource_identifier_mechanism;

-- However, the uniqueness constraint doesn't enforce uniqueness when one
-- of the fields is null, and one of these fields -- resource_id -- is
-- _usually_ null. So we need a unique partial index to properly enforce
-- the constraint.
CREATE UNIQUE INDEX if not exists ix_licensepooldeliveries_unique_when_no_resource ON public.licensepooldeliveries USING btree (data_source_id, identifier_id, delivery_mechanism_id) WHERE (resource_id IS NULL);


-- deliverymechanisms doesn't have a uniqueness constraint, just a unique index.
-- Let's change it to a uniqueness constraint (which comes with an implicit
-- index) and then add a conditional unique index.
drop index if exists ix_deliverymechanisms_drm_scheme_content_type;

ALTER TABLE deliverymechanisms ADD CONSTRAINT deliverymechanisms_content_type_drm_scheme UNIQUE (content_type, drm_scheme);

CREATE UNIQUE INDEX if not exists ix_deliverymechanisms_unique_when_no_drm ON public.deliverymechanisms USING btree (content_type) WHERE (drm_scheme IS NULL);
