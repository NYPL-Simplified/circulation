# encoding: utf-8
import pytest

import os
from StringIO import StringIO
from zipfile import ZipFile
from core.testing import DatabaseTest
from . import sample_data
from api.feedbooks import (
    FeedbooksOPDSImporter,
    FeedbooksImportMonitor,
    RehostingPolicy,
)
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Representation,
    RightsStatus,
)
from core.model.configuration import ExternalIntegrationLink
from core.metadata_layer import (
    Metadata,
    LinkData,
)
from core.opds import OPDSFeed
from core.s3 import MockS3Uploader
from core.testing import (
    DummyHTTPClient,
    DummyMetadataClient,
)

LIFE_PLUS_70 = "This work is available for countries where copyright is Life+70."

class TestFeedbooksOPDSImporter(DatabaseTest):

    def _importer(self, **settings):
        collection = self._collection(
            name=DataSource.FEEDBOOKS + self._str,
            protocol=ExternalIntegration.FEEDBOOKS,
        )

        defaults = {
            FeedbooksOPDSImporter.REALLY_IMPORT_KEY: "true",
            FeedbooksOPDSImporter.REPLACEMENT_CSS_KEY: None,
        }
        for setting, value in defaults.items():
            if setting not in settings:
                settings[setting] = value

        collection.external_account_id = settings.pop('language', 'de')
        for setting, value in settings.items():
            if value is None:
                continue
            collection.external_integration.set_setting(setting, value)

        return collection, FeedbooksOPDSImporter(
            self._db, collection,
            http_get=self.http.do_get, mirrors=self.mirrors,
            metadata_client=self.metadata,
        )

    def setup_method(self):
        super(TestFeedbooksOPDSImporter, self).setup_method()
        self.http = DummyHTTPClient()
        self.metadata = DummyMetadataClient()
        self.mirrors = dict(covers_mirror=MockS3Uploader(),books_mirror=MockS3Uploader())

        self.data_source = DataSource.lookup(self._db, DataSource.FEEDBOOKS)

        # Create a default importer that's good enough for most tests.
        self.collection, self.importer = self._importer()

    def sample_file(self, filename):
        return sample_data(filename, "feedbooks")

    def test_safety_switch(self):
        """The importer won't be instantiated if REALLY_IMPORT_KEY is not
        set to true.
        """
        settings = {FeedbooksOPDSImporter.REALLY_IMPORT_KEY: "false"}
        with pytest.raises(Exception) as excinfo:
            self._importer(**settings)
        assert "configured to not actually do an import" in str(excinfo.value)

    def test_unique_identifier(self):
        # The unique account ID is the language of the Feedbooks
        # feed in use.
        assert 'de' == self.collection.unique_account_id

    def test_error_retrieving_replacement_css(self):
        # The importer cannot be instantiated if a replacement CSS
        # is specified but the replacement CSS document cannot be
        # retrieved or does not appear to be CSS.

        settings = {FeedbooksOPDSImporter.REPLACEMENT_CSS_KEY: "http://foo"}

        self.http.queue_response(500, content="An error message")
        with pytest.raises(IOError) as excinfo:
            self._importer(**settings)
        assert "Replacement stylesheet URL returned 500 response code" in str(excinfo.value)

        self.http.queue_response(
            200, content="We have many CSS offerings",
            media_type="text/html"
        )
        with pytest.raises(IOError) as excinfo:
            self._importer(**settings)
        assert "Replacement stylesheet is 'text/html', not a CSS document." in str(excinfo.value)

    def test_extract_feed_data_improves_descriptions(self):
        feed = self.sample_file("feed.atom")
        self.http.queue_response(200, OPDSFeed.ENTRY_TYPE,
                                 content=self.sample_file("677.atom"))
        metadata, failures = self.importer.extract_feed_data(
            feed, "http://url/"
        )
        [(key, value)] = metadata.items()
        assert u'http://www.feedbooks.com/book/677' == key
        assert "Discourse on the Method" == value.title

        # Instead of the short description from feed.atom, we have the
        # long description from 677.atom.
        [description] = [x for x in value.links if x.rel==Hyperlink.DESCRIPTION]
        assert 1818 == len(description.content)

    def test_improve_description(self):
        # Here's a Metadata that has a bad (truncated) description.
        metadata = Metadata(self.data_source)

        bad_description = LinkData(rel=Hyperlink.DESCRIPTION, media_type="text/plain", content=u"The Discourse on the Method is a philosophical and mathematical treatise published by Ren\xe9 Descartes in 1637. Its full name is Discourse on the Method of Rightly Conducting the Reason, and Searching for Truth in the Sciences (French title: Discour...")

        irrelevant_description = LinkData(
            rel=Hyperlink.DESCRIPTION, media_type="text/plain",
            content="Don't look at me; I'm irrelevant!"
        )

        # Sending an HTTP request to this URL is going to give a 404 error.
        alternate = LinkData(rel=Hyperlink.ALTERNATE, href="http://foo/",
                             media_type=OPDSFeed.ENTRY_TYPE)

        # We're not even going to try to send an HTTP request to this URL
        # because it doesn't promise an OPDS entry.
        alternate2 = LinkData(rel=Hyperlink.ALTERNATE, href="http://bar/",
                             media_type="text/html")

        # But this URL will give us full information about this
        # entry, including a better description.
        alternate3 = LinkData(
            rel=Hyperlink.ALTERNATE, href="http://baz/",
            media_type=OPDSFeed.ENTRY_TYPE
        )

        # This URL will not be requested because the third alternate URL
        # gives us the answer we're looking for.
        alternate4 = LinkData(
            rel=Hyperlink.ALTERNATE, href="http://qux/",
            media_type=OPDSFeed.ENTRY_TYPE
        )

        # Two requests will be made. The first will result in a 404
        # error. The second will give us an OPDS entry.
        self.http.queue_response(404, content="Not found")
        self.http.queue_response(200, OPDSFeed.ENTRY_TYPE,
                                 content=self.sample_file("677.atom"))

        metadata.links = [bad_description, irrelevant_description,
                          alternate, alternate2, alternate3, alternate4]

        self.importer.improve_description("some ID", metadata)

        # The descriptions have been removed from metatadata.links,
        # because 677.atom included a description we know was better.
        #
        # The incomplete description was removed even though 677.atom
        # also included a copy of it.
        assert bad_description not in metadata.links
        assert irrelevant_description not in metadata.links

        # The more complete description from 677.atom has been added.
        [good_description] = [
            x for x in metadata.links if x.rel == Hyperlink.DESCRIPTION
        ]

        # The four alternate links have not been touched.
        assert (alternate in metadata.links)
        assert (alternate2 in metadata.links)
        assert (alternate3 in metadata.links)
        assert (alternate4 in metadata.links)

        # Two HTTP requests were made.
        assert ['http://foo/', 'http://baz/'] == self.http.requests

    def test_generic_acquisition_epub_link_picked_up_as_open_access(self):
        """The OPDS feed has links with generic OPDS "acquisition"
        relations. We know that the EPUB link should be open-access
        relations, and we modify its relation on the way in.

        We do not modify the link relation for links to the other
        formats, which means they don't get picked up at all.
        """

        feed = self.sample_file("feed_with_open_access_book.atom")
        imports, errors = self.importer.extract_feed_data(feed)
        [book] = imports.values()
        open_access_links = [x for x in book.circulation.links
                             if x.rel==Hyperlink.OPEN_ACCESS_DOWNLOAD]
        links = sorted(x.href for x in open_access_links)
        assert ['http://www.feedbooks.com/book/677.epub'] == links

        generic_links = [x for x in book.circulation.links
                         if x.rel==Hyperlink.GENERIC_OPDS_ACQUISITION]
        assert [] == generic_links

    def test_open_access_book_modified_and_mirrored(self):
        # If no replacement CSS is specified (this is the case with
        # the default importer), the OPDSImporter.content_modifier
        # method is not assigned.
        assert None == self.importer.new_css
        assert None == self.importer.content_modifier

        # Let's create an importer that does specify a replacement
        # CSS file.
        settings = {
            FeedbooksOPDSImporter.REPLACEMENT_CSS_KEY : "http://css/"
        }

        # The very first request made is going to be to the
        # REPLACEMENT_CSS_KEY URL.
        self.http.queue_response(
            200, content="Some new CSS", media_type="text/css",
        )
        ignore, importer = self._importer(**settings)

        # The replacement CSS is retrieved during the FeedbooksImporter
        # constructor.
        assert [u'http://css/'] == self.http.requests

        # OPDSImporter.content_modifier has been set to call replace_css
        # when necessary.
        assert "Some new CSS" == importer.new_css
        assert importer.replace_css == importer.content_modifier

        # The requests to the various copies of the book will succeed,
        # and the books will be mirrored.
        self.http.queue_response(
            200, content=self.sample_file("677.epub"),
            media_type=Representation.EPUB_MEDIA_TYPE
        )

        # The request to
        # http://covers.feedbooks.net/book/677.jpg?size=large&t=1428398185'
        # will result in a 404 error, and the image will not be
        # mirrored.
        self.http.queue_response(404, media_type="text/plain")

        self.metadata.lookups = { u"René Descartes" : "Descartes, Rene" }
        feed = self.sample_file("feed_with_open_access_book.atom")
        self.http.queue_response(
            200, OPDSFeed.ACQUISITION_FEED_TYPE,
            content=feed
        )

        [edition], [pool], [work], failures = importer.import_from_feed(feed)

        assert {} == failures

        # The work has been created and has metadata.
        assert "Discourse on the Method" == work.title
        assert u'Ren\xe9 Descartes' == work.author

        # Two more mock HTTP requests have now made.
        assert ([
            u'http://css/',
            u'http://www.feedbooks.com/book/677.epub',
            u'http://covers.feedbooks.net/book/677.jpg?size=large&t=1428398185',
        ] ==
            self.http.requests)

        # The EPUB was 'uploaded' to the mock S3 service and turned
        # into a LicensePoolDeliveryMechanism. The other formats were
        # ignored.
        [mechanism] = pool.delivery_mechanisms
        assert (
            mechanism.resource.representation.mirror_url ==
            'https://test-content-bucket.s3.amazonaws.com/FeedBooks/URI/http%3A//www.feedbooks.com/book/677/Discourse%20on%20the%20Method.epub')
        assert u'application/epub+zip' == mechanism.delivery_mechanism.content_type

        # From information contained in the OPDS entry we determined
        # the book's license to be CC-BY-NC.
        assert (u'https://creativecommons.org/licenses/by-nc/4.0' ==
            mechanism.rights_status.uri)

        # The pool is marked as open-access, because it has an open-access
        # delivery mechanism that was mirrored.
        assert True == pool.open_access

        # The mirrored content contains the modified CSS in the books mirror
        # due to the link rel type.
        content = StringIO(self.mirrors[ExternalIntegrationLink.OPEN_ACCESS_BOOKS].content[0])
        with ZipFile(content) as zip:
            # The zip still contains the original epub's files.
            assert "META-INF/container.xml" in zip.namelist()
            assert "OPS/css/about.css" in zip.namelist()
            assert "OPS/main0.xml" in zip.namelist()

            # The content of an old file hasn't changed.
            with zip.open("mimetype") as f:
                assert "application/epub+zip\r\n" == f.read()

            # The content of CSS files has been changed to the new value.
            with zip.open("OPS/css/about.css") as f:
                assert "Some new CSS" == f.read()

    def test_in_copyright_book_not_mirrored(self):

        self.metadata.lookups = { u"René Descartes" : "Descartes, Rene" }
        feed = self.sample_file("feed_with_in_copyright_book.atom")
        self.http.queue_response(
            200, OPDSFeed.ACQUISITION_FEED_TYPE,
            content=feed
        )

        [edition], [pool], [work], failures = self.importer.import_from_feed(feed)

        # The work has been created and has metadata.
        assert "Discourse on the Method" == work.title
        assert u'Ren\xe9 Descartes' == work.author

        # No mock HTTP requests were made.
        assert [] == self.http.requests

        # Nothing was uploaded to the mock S3 covers mirror.
        assert [] == self.mirrors[ExternalIntegrationLink.COVERS].uploaded

        # The LicensePool's delivery mechanism is set appropriately
        # to reflect an in-copyright work.
        [mechanism] = pool.delivery_mechanisms
        assert RightsStatus.IN_COPYRIGHT == mechanism.rights_status.uri

        # The DeliveryMechanism has a Representation but the Representation
        # has not been set as mirrored, because nothing was uploaded.
        rep = mechanism.resource.representation
        assert 'http://www.feedbooks.com/book/677.epub' == rep.url
        assert None == rep.mirror_url
        assert None == rep.mirror_exception

        # The pool is not marked as open-access because although it
        # has open-access links, they're not licensed under terms we
        # can use.
        assert False == pool.open_access


class TestRehostingPolicy(object):

    def test_rights_uri(self):
        # A Feedbooks work based on a text that is in copyright in the
        # US gets a RightsStatus of IN_COPYRIGHT.  We will not be
        # hosting this book and if we should host it by accident we
        # will not redistribute it.
        pd_in_australia_only = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", 1930
        )
        assert RightsStatus.IN_COPYRIGHT == pd_in_australia_only

        unknown_australia_publication = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", None
        )
        assert RightsStatus.IN_COPYRIGHT == unknown_australia_publication

        # A Feedbooks work based on a text that is in the US public
        # domain is relicensed to us as CC-BY-NC.
        pd_in_us = RehostingPolicy.rights_uri(
            LIFE_PLUS_70, "gutenberg.net.au", 1922
        )
        assert RightsStatus.CC_BY_NC == pd_in_us

        # A Feedbooks work based on a text whose CC license is not
        # compatible with CC-BY-NC is relicensed to us under the
        # original license.
        sharealike = RehostingPolicy.rights_uri(
            "Attribution Share Alike (cc by-sa)", "mywebsite.com", 2016
        )
        assert RightsStatus.CC_BY_SA == sharealike

        # A Feedbooks work based on a text whose rights status cannot
        # be determined gets an unknown RightsStatus. We will not be
        # hosting this book, but we might change our minds after
        # investigating.
        unknown = RehostingPolicy.rights_uri(
            RehostingPolicy.RIGHTS_UNKNOWN, "mywebsite.com", 2016
        )
        assert RightsStatus.UNKNOWN == unknown

    def test_can_rehost_us(self):
        # We will rehost anything published prior to 1923.
        assert (
            True == RehostingPolicy.can_rehost_us(
                LIFE_PLUS_70, "gutenberg.net.au", 1922
            ))

        # We will rehost anything whose rights statement explicitly
        # indicates it can be rehosted in the US, no matter the
        # issuance date.
        for terms in RehostingPolicy.CAN_REHOST_IN_US:
            assert (
                True == RehostingPolicy.can_rehost_us(
                    terms, "gutenberg.net.au", 2016
                ))

        # We will rehost anything that originally derives from a
        # US-based site that specializes in open-access books.
        for site in list(RehostingPolicy.US_SITES) + [
                "WikiSource", "Gutenberg", "http://gutenberg.net/"
        ]:
            assert (
                True == RehostingPolicy.can_rehost_us(
                    None, site, 2016
                ))

        # If none of these conditions are met we will not rehost a
        # book.
        assert (
            False == RehostingPolicy.can_rehost_us(
                LIFE_PLUS_70, "gutenberg.net.au", 1930
            ))

        # If a book would require manual work to determine copyright
        # status, we will distinguish slightly between that case and
        # the case where we're pretty sure.
        assert (
            None == RehostingPolicy.can_rehost_us(
                RehostingPolicy.RIGHTS_UNKNOWN, "Some random website", 2016
            ))


class TestFeedbooksImportMonitor(DatabaseTest):

    def test_subclass_methods(self):
        """Test methods of OPDSImportMonitor overridden with special
        Feedbooks logic.
        """
        collection = self._collection(protocol=ExternalIntegration.FEEDBOOKS)
        collection.external_account_id = "somelanguage"
        collection.external_integration.set_setting(
            FeedbooksOPDSImporter.REALLY_IMPORT_KEY, "true"
        )

        monitor = FeedbooksImportMonitor(
            self._db, collection, import_class=FeedbooksOPDSImporter,
        )

        # The data source and protocol are always Feedbooks.
        assert DataSource.FEEDBOOKS == monitor.data_source(collection)
        assert monitor.PROTOCOL == ExternalIntegration.FEEDBOOKS

        # The URL is always a feedbooks.com URL based on the collection's
        # language setting.
        assert (u"http://www.feedbooks.com/books/recent.atom?lang=somelanguage" ==
            monitor.opds_url(collection))
