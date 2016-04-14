from nose.tools import set_trace

from api.opds import CirculationManagerAnnotator
from core.lane import Facets, Pagination
from core.model import BaseMaterializedWork, LicensePool
from core.opds import AcquisitionFeed

class AdminAnnotator(CirculationManagerAnnotator):

    def __init__(self, circulation, test_mode=False):
        super(AdminAnnotator, self).__init__(circulation, None, test_mode=test_mode)

    def annotate_work_entry(self, work, active_license_pool, edition, identifier, feed, entry):

        super(AdminAnnotator, self).annotate_work_entry(work, active_license_pool, edition, identifier, feed, entry)

        if isinstance(work, BaseMaterializedWork):
            identifier_identifier = work.identifier
            data_source_name = work.name
        else:
            identifier_identifier = identifier.identifier
            data_source_name = active_license_pool.data_source.name

        feed.add_link_to_entry(
            entry,
            rel="http://librarysimplified.org/terms/rel/refresh",
            href=self.url_for(
                "refresh", data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
        )

        if active_license_pool.suppressed:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/restore",
                href=self.url_for(
                    "unsuppress", data_source=data_source_name,
                    identifier=identifier_identifier, _external=True)
            )
        else:
            feed.add_link_to_entry(
                entry,
                rel="http://librarysimplified.org/terms/rel/hide",
                href=self.url_for(
                    "suppress", data_source=data_source_name,
                    identifier=identifier_identifier, _external=True)
            )

        feed.add_link_to_entry(
            entry,
            rel="edit",
            href=self.url_for(
                "edit", data_source=data_source_name,
                identifier=identifier_identifier, _external=True)
        )
            
    def complaints_url(self, facets, pagination):
        kwargs = dict(facets.items())
        kwargs.update(dict(pagination.items()))
        return self.url_for("complaints", _external=True, **kwargs)

    def suppressed_url(self, pagination):
        kwargs = dict(pagination.items())
        return self.url_for("suppressed", _external=True, **kwargs)

    def annotate_feed(self, feed):
        # Add a 'search' link.
        search_url = self.url_for(
            'lane_search', languages=None,
            _external=True
        )
        search_link = dict(
            rel="search",
            type="application/opensearchdescription+xml",
            href=search_url
        )
        feed.add_link(**search_link)


class AdminFeed(AcquisitionFeed):

    @classmethod
    def complaints(cls, _db, title, url, annotator, pagination=None):
        facets = Facets.default()
        pagination = pagination or Pagination.default()

        q = LicensePool.with_complaint(_db)
        results = pagination.apply(q).all()

        if len(results) > 0:
            (pools, counts) = zip(*results)
        else:
            pools = ()

        works = [pool.work for pool in pools]
        feed = cls(_db, title, url, works, annotator)

        # Render a 'start' link
        top_level_title = "Collection Home"
        start_uri = annotator.groups_url(None)
        feed.add_link(href=start_uri, rel="start", title=top_level_title)

        if len(works) > 0:
            # There are works in this list. Add a 'next' link.
            feed.add_link(rel="next", href=annotator.complaints_url(facets, pagination.next_page))

        if pagination.offset > 0:
            feed.add_link(rel="first", href=annotator.complaints_url(facets, pagination.first_page))

        previous_page = pagination.previous_page
        if previous_page:
            feed.add_link(rel="previous", href=annotator.complaints_url(facets, previous_page))

        annotator.annotate_feed(feed)
        return unicode(feed)

    @classmethod
    def suppressed(cls, _db, title, url, annotator, pagination=None):
        pagination = pagination or Pagination.default()

        q = _db.query(LicensePool).filter(LicensePool.suppressed == True)
        pools = pagination.apply(q).all()

        works = [pool.work for pool in pools]
        feed = cls(_db, title, url, works, annotator)

        # Render a 'start' link
        top_level_title = "Collection Home"
        start_uri = annotator.groups_url(None)
        feed.add_link(href=start_uri, rel="start", title=top_level_title)

        if len(works) > 0:
            # There are works in this list. Add a 'next' link.
            feed.add_link(rel="next", href=annotator.suppressed_url(pagination.next_page))

        if pagination.offset > 0:
            feed.add_link(rel="first", href=annotator.suppressed_url(pagination.first_page))

        previous_page = pagination.previous_page
        if previous_page:
            feed.add_link(rel="previous", href=annotator.suppressed_url(previous_page))

        annotator.annotate_feed(feed)
        return unicode(feed)
            
