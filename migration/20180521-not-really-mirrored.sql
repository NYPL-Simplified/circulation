-- Some representations have mirror_url set even though they were
-- never mirrored. Their mirror_urls should be blanked out.

-- This shouldn't have happened, but just in case, so we don't lose information.
update representations set url=mirror_url where url is null and mirror_url is not null;

update representations set mirror_url=null where url is not null and mirrored_at is null and mirror_url is not null;
