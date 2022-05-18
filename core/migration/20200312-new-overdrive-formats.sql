DO $$
 BEGIN
  -- Add the new delivery mechanisms we'll be adding onto certain
  -- LicensePools.
  BEGIN
   insert into deliverymechanisms (
    content_type, drm_scheme, default_client_can_fulfill
   ) values (
    'application/vnd.overdrive.circulation.api+json;profile=audiobook',
    'Libby DRM', 't'
   );
  EXCEPTION
    WHEN unique_violation THEN RAISE NOTICE 'Audiobook manifest delivery mechanism already exists, not adding it.';
  END;

  BEGIN
   insert into deliverymechanisms (
    content_type, drm_scheme, default_client_can_fulfill
   ) values (
    'application/vnd.overdrive.circulation.api+json;profile=ebook',
    'Libby DRM', 'f'
   );
  EXCEPTION
    WHEN unique_violation THEN RAISE NOTICE 'Ebook manifest delivery mechanism already exists, not adding it.';
  END;

  -- Before we fix the Streaming/Overdrive inaccuracy, let's exploit it
  -- to add new delivery mechanisms.

  -- If an audiobook is available streaming through Overdrive Listen,
  -- it's also available through a manifest. Add rows to licensepooldeliveries
  -- to represent the available manifests.
  BEGIN
   insert into licensepooldeliveries (
    data_source_id, identifier_id, delivery_mechanism_id
   ) select
    ld.data_source_id, ld.identifier_id,
    (select id from deliverymechanisms
      where
       content_type='application/vnd.overdrive.circulation.api+json;profile=audiobook'
       and drm_scheme='Libby DRM'
     )
   from licensepooldeliveries ld join deliverymechanisms d on ld.delivery_mechanism_id=d.id
    where d.content_type='Streaming Audio' and d.drm_scheme='Overdrive DRM'
   ;
 END;

  -- Do the same thing for ebooks that are available streaming through
  -- Overdrive Read, it's also available through a manifest. Add
  -- rows to licensepooldeliveries to represent the available
  -- manifests.
  BEGIN
   insert into licensepooldeliveries (
    data_source_id, identifier_id, delivery_mechanism_id
   ) select
    ld.data_source_id, ld.identifier_id,
    (select id from deliverymechanisms
      where
       content_type='application/vnd.overdrive.circulation.api+json;profile=ebook'
       and drm_scheme='Libby DRM'
     )
   from licensepooldeliveries ld join deliverymechanisms d on ld.delivery_mechanism_id=d.id
    where d.content_type='Streaming Text' and d.drm_scheme='Overdrive DRM'
   ;
 END;

 -- Finally we can fix this little inaccuracy. Content streamed from
 -- Overdrive is not encrypted through Overdrive DRM, either generally
 -- or in the specific sense of the DRM system employed by the mobile
 -- app called 'Overdrive'. The streaming format is, itself, the 'DRM'
 -- in play.
 BEGIN
  update deliverymechanisms set drm_scheme='Streaming'
     where drm_scheme='Overdrive DRM' and content_type in (
         'Streaming Text', 'Streaming Audio', 'Streaming Video'
     );
  EXCEPTION
    WHEN unique_violation THEN RAISE NOTICE 'New Overdrive streaming audio options already exist; not renaming the old ones. You should probably delete the old ones.';
 END;
END;
$$;
