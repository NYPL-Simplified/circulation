from nose.tools import (
    eq_,
    set_trace,
)

from copy import deepcopy
import datetime

from metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    SubjectData,
)

from model import (
    DataSource,
    DeliveryMechanism,
    Hyperlink, 
    Identifier,
    Representation,
    RightsStatus,
    Subject,
)

from . import (
    DatabaseTest,
)


class TestCirculationData(DatabaseTest):

    def test_metadata_can_be_deepcopied(self):
        # Check that we didn't put something in the CirculationData that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(Subject.TAG, "subject")
        contributor = ContributorData()
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        link = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        format = FormatData(Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        rights_uri = RightsStatus.GENERIC_OPEN_ACCESS

        circulation_data = CirculationData(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=[link],
            licenses_owned=5,
            licenses_available=5,
            licenses_reserved=None,
            patrons_in_hold_queue=None,
            formats=[format],
            default_rights_uri=rights_uri,
        )

        circulation_data_copy = deepcopy(circulation_data)

        # If deepcopy didn't throw an exception we're ok.
        assert circulation_data_copy is not None


    def test_book_info_with_circulation_data(self):
        # Tests that can convert an overdrive json block into a CirculationData object.
        # Originally from TestOverdriveRepresentationExtractor.

        """
        raw, info = self.sample_json("overdrive_metadata.json")
        metadata = OverdriveRepresentationExtractor.book_info_to_circulation_data(info)

        [...]

        # Available formats.
        [kindle, pdf] = sorted(metadata.formats, key=lambda x: x.content_type)
        eq_(DeliveryMechanism.KINDLE_CONTENT_TYPE, kindle.content_type)
        eq_(DeliveryMechanism.KINDLE_DRM, kindle.drm_scheme)

        eq_(Representation.PDF_MEDIA_TYPE, pdf.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, pdf.drm_scheme)



    from: OPDSImporter
    def detail_for_feedparser_entry(cls, entry):
    	[...]
        '''
        # metadata no longer knows about circulation
        if added_to_collection_time:
            circulation = CirculationData(
                licenses_owned=None, licenses_available=None,
                licenses_reserved=None, patrons_in_hold_queue=None,
                first_appearance=added_to_collection_time,
                data_source=...
            )
        else:
            circulation = None
        '''
        [...]
        kwargs = dict(
            license_data_source=license_data_source,
            title=title,
            subtitle=subtitle,
            language=language,
            publisher=publisher,
            links=links,
            rights_uri=rights_uri,
            last_update_time=last_update_time,
            #circulation=circulation,
        )
        return identifier, kwargs, status_message


write test for opds_import.    @classmethod
    def extract_circulationdata_from_feedparser(cls, feed):






class TestCirculationData(DatabaseTest):

    def test_links_filtered(self):
        # TODO: Tests that passed-in links filter down to only the relevant ones.
        links = []
        def summary_to_linkdata(detail):
            if not detail:
                return None
            if not 'value' in detail or not detail['value']:
                return None

            content = detail['value']
            media_type = detail.get('type', 'text/plain')
            return LinkData(
                rel=Hyperlink.DESCRIPTION,
                media_type=media_type,
                content=content
            )

        summary_detail = entry.get('summary_detail', None)
        link = summary_to_linkdata(summary_detail)
        if link:
            links.append(link)

        for content_detail in entry.get('content', []):
            link = summary_to_linkdata(content_detail)
            if link:
                links.append(link)

        kwargs_circ = dict(
            # Note: later on, we'll check to make sure data_source is lendable, and if not, abort creating a pool and a work.
            data_source=data_source,
            links=links,
            # Note: CirculationData.default_rights_uri is not same as the old 
            # Metadata.rights_uri, but we're treating it same for now.
            default_rights_uri=rights_uri,
            last_checked=last_update_time, 
            # first appearance in our databases, 
            # gets assigned to pool, if have to make new pool. 
            first_appearance = datetime.datetime.utcnow()
        )
        circulation_data = CirculationData(**kwargs_circ)
        pass




    def test_open_access_content_mirrored(self):
        # TODO: mirroring links is now also CirculationData's job.  So the unit tests 
        # that test for that have been changed to call to mirror cover images.
        # However, updated tests passing does not guarantee that all code now 
        # correctly calls on CirculationData, too.  This is a risk.

        # Make sure that open access material links are translated to our S3 buckets, and that 
        # commercial material links are left as is.

        mirror = DummyS3Uploader()
        # Here's a book.
        edition, pool = self._edition(with_license_pool=True)

        # Here's a link to the content of the book, which will be mirrored.
        link_mirrored = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://example.com/",
            media_type=Representation.EPUB_MEDIA_TYPE,
            content="i am a tiny book"
        )

        # This link will not be mirrored.
        link_unmirrored = LinkData(
            rel=Hyperlink.SAMPLE, href="http://example.com/2",
            media_type=Representation.TEXT_PLAIN,
            content="i am a tiny (This is a sample. To read the rest of this book, please visit your local library.)"
        )

        # Apply the metadata.
        policy = ReplacementPolicy(mirror=mirror)
        metadata = Metadata(links=[link_mirrored, link_unmirrored], data_source=edition.data_source)
        metadata.apply(edition, replace=policy)
        

        # Only the open-access link has been 'mirrored'.
        # TODO: make sure the refactor is done right, and metadata does not upload
        #eq_(0, len(mirror.uploaded))
        [book] = mirror.uploaded

        # TODO: make sure the refactor is done right, and circulation does upload 
        #circulation = CirculationData(links=[link_mirrored, link_unmirrored], data_source=edition.data_source)
        #circulation.apply(pool, replace=policy)
        #[book] = mirror.uploaded

        # It's remained an open-access link.
        eq_(
            [Hyperlink.OPEN_ACCESS_DOWNLOAD], 
            [x.rel for x in book.resource.links]
        )


        # It's been 'mirrored' to the appropriate S3 bucket.
        assert book.mirror_url.startswith('http://s3.amazonaws.com/test.content.bucket/')
        expect = '/%s/%s.epub' % (
            edition.primary_identifier.identifier,
            edition.title
        )
        assert book.mirror_url.endswith(expect)

        # make sure the mirrored link is safely on edition
        sorted_edition_links = sorted(edition.license_pool.identifier.links, key=lambda x: x.rel)
        mirrored_representation, unmirrored_representation = [edlink.resource.representation for edlink in sorted_edition_links]
        assert mirrored_representation.mirror_url.startswith('http://s3.amazonaws.com/test.content.bucket/')

        # make sure the unmirrored link is safely on edition
        eq_('http://example.com/2', unmirrored_representation.url)
        # make sure the unmirrored link has not been translated to an S3 URL
        eq_(None, unmirrored_representation.mirror_url)


    def test_mirror_open_access_link_fetch_failure(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirror = DummyS3Uploader()
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirror=mirror, http_get=h.do_get)

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            license_pool=pool, media_type=link.media_type,
            content=link.content,
        )
        h.queue_response(403)
        
        m.mirror_link(pool, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # Fetch failed, so we should have a fetch exception but no mirror url.
        assert representation.fetch_exception != None
        eq_(None, representation.mirror_exception)
        eq_(None, representation.mirror_url)
        eq_(link.href, representation.url)
        assert representation.fetched_at != None
        eq_(None, representation.mirrored_at)

        # The license pool is suppressed when fetch fails.
        eq_(True, pool.suppressed)
        assert representation.fetch_exception in pool.license_exception

    def test_mirror_open_access_link_mirror_failure(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirror = DummyS3Uploader(fail=True)
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirror=mirror, http_get=h.do_get)

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href=self._url,
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            license_pool=pool, media_type=link.media_type,
            content=link.content,
        )

        h.queue_response(200, media_type=Representation.EPUB_MEDIA_TYPE)
        
        m.mirror_link(pool, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # The representation was fetched successfully.
        eq_(None, representation.fetch_exception)
        assert representation.fetched_at != None

        # But mirroing failed.
        assert representation.mirror_exception != None
        eq_(None, representation.mirrored_at)
        eq_(link.media_type, representation.media_type)
        eq_(link.href, representation.url)

        # The mirror url should still be set.
        assert "Gutenberg" in representation.mirror_url
        assert representation.mirror_url.endswith("%s.epub" % edition.title)

        # Book content is still there since it wasn't mirrored.
        assert representation.content != None

        # The license pool is suppressed when mirroring fails.
        eq_(True, pool.suppressed)
        assert representation.mirror_exception in pool.license_exception





"""

