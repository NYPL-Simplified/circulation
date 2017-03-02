alter table licensepools add open_access_download_url character varying;
update licensepools set open_access_download_url = subq.url from (
       select e.id as eid, e.open_access_download_url as url
       from editions e
       join licensepools lp on lp.identifier_id=e.primary_identifier_id
       where e.open_access_download_url is not null
)
as subq where licensepools.presentation_edition_id=subq.eid;

-- It doesn't hurt anything to keep editions.open_access_download_url
-- around for a while, in case there's been a mistake.
-- alter table editions drop open_access_download_url;
