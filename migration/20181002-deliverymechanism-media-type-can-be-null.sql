-- If a DRM document is also the manifest document, a delivery
-- mechanism has no separate content type.
ALTER TABLE deliverymechanisms ALTER COLUMN content_type DROP NOT NULL;

-- MP3 files are not a content type for purposes of describing
-- fulfillment, because they are always obtained by processing a
-- manifest. In that case the manifest document is the content type.
update deliverymechanisms set content_type=NULL where content_type='audio/mpeg' and drm_scheme='application/vnd.librarysimplified.findaway.license+json';
