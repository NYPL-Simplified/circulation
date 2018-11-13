-- If an audio/mpeg+findaway delivery mechanism was created again after the old
-- one was removed, change over all rows in licensepooldeliveries that use it.
update licensepooldeliveries set delivery_mechanism_id=(
  select id from deliverymechanisms where content_type is null and drm_scheme='application/vnd.librarysimplified.findaway.license+json'
) where delivery_mechanism_id = (
  select id from deliverymechanisms where content_type='audio/mpeg' and drm_scheme='application/vnd.librarysimplified.findaway.license+json'
);

-- Then delete it.
delete from deliverymechanisms where content_type='audio/mpeg' and drm_scheme='application/vnd.librarysimplified.findaway.license+json';
