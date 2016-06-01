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
    ReplacementPolicy,
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
    DummyHTTPClient,
)

from s3 import DummyS3Uploader

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


    def test_links_filtered(self):
        # Tests that passed-in links filter down to only the relevant ones.
        link1 = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        link2 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        link3 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        link4 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        link5 = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/", thumbnail=link4,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        links = [link1, link2, link3, link4, link5]

        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        circulation_data = CirculationData(
            DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=links,
        )

        filtered_links = sorted(circulation_data.links, key=lambda x:x.rel)

        eq_([link1], filtered_links)


    def test_open_access_content_mirrored(self):
        # Make sure that open access material links are translated to our S3 buckets, and that 
        # commercial material links are left as is.
        # Note: Mirroring tests passing does not guarantee that all code now 
        # correctly calls on CirculationData, as well as Metadata.  This is a risk.

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
            rel=Hyperlink.DRM_ENCRYPTED_DOWNLOAD, href="http://example.com/2",
            media_type=Representation.EPUB_MEDIA_TYPE,
            content="i am a pricy book"
        )

        # Apply the metadata.
        policy = ReplacementPolicy(mirror=mirror)
        circulation_data = CirculationData(
        	data_source=edition.data_source, 
        	primary_identifier=edition.primary_identifier,
        	links=[link_mirrored, link_unmirrored],
        )
        circulation_data.apply(pool, replace=policy)
        

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
        unmirrored_representation, mirrored_representation = [edlink.resource.representation for edlink in sorted_edition_links]
        assert mirrored_representation.mirror_url.startswith('http://s3.amazonaws.com/test.content.bucket/')

        # make sure the unmirrored link is safely on edition
        eq_('http://example.com/2', unmirrored_representation.url)
        # make sure the unmirrored link has not been translated to an S3 URL
        eq_(None, unmirrored_representation.mirror_url)


    def test_mirror_open_access_link_fetch_failure(self):
        mirror = DummyS3Uploader()
        h = DummyHTTPClient()

        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy(mirror=mirror, http_get=h.do_get)
        circulation_data = CirculationData(
        	data_source=edition.data_source, 
        	primary_identifier=edition.primary_identifier,
        )

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
        
        circulation_data.mirror_link(pool, data_source, link, link_obj, policy)

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
        mirror = DummyS3Uploader(fail=True)
        h = DummyHTTPClient()

        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        policy = ReplacementPolicy(mirror=mirror, http_get=h.do_get)

        circulation_data = CirculationData(
        	data_source=edition.data_source, 
        	primary_identifier=edition.primary_identifier,
        )

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
        
        circulation_data.mirror_link(pool, data_source, link, link_obj, policy)

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






