-- Some representations have mirror_url set even though they were
-- never mirrored. Their mirror_urls should be blanked out.

-- This shouldn't have happened, but just in case, so we don't lose information.
update representations set url=mirror_url where url is null and mirror_url is not null;

update representations set mirror_url=null where url is not null and mirrored_at is null and mirror_url is not null;

-- A representation is never 'mirrored' to its original URL.
-- More likely Representation.set_as_mirrored() was called, but we now
-- prefer to leave mirror_url alone.
update representations set mirror_url=null, mirrored_at=null where url=mirror_url;
