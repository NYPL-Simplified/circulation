# encoding: utf-8
import os
import pytest

from ...config import Configuration
from ...testing import DummyHTTPClient
from ...model import create
from ...model.datasource import DataSource
from ...model.edition import Edition
from ...model.identifier import Identifier
from ...model.licensing import RightsStatus
from ...model.resource import (
    Hyperlink,
    Representation,
    Resource,
)
from ...testing import MockRequestsResponse


class TestHyperlink:

    def test_add_link(self, db_session, create_edition):
        """
        GIVEN: A LicensePool
        WHEN:  Adding a link between this LicensePool and a resource
        THEN:  The LicensePool and resource are linked
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        identifier = edition.primary_identifier
        data_source = pool.data_source
        original, _ = create(db_session, Resource, url="http://example.com")
        hyperlink, is_new = pool.add_link(
            Hyperlink.DESCRIPTION, "http://example.com/", data_source,
            "text/plain", "The content", None, RightsStatus.CC_BY,
            "The rights explanation", original,
            transformation_settings=dict(setting="a setting"))

        assert is_new is True

        rep = hyperlink.resource.representation
        assert rep.media_type == "text/plain"
        assert rep.content == b"The content"
        assert hyperlink.rel == Hyperlink.DESCRIPTION
        assert hyperlink.identifier == identifier
        assert hyperlink.resource.rights_status.uri == RightsStatus.CC_BY
        assert hyperlink.resource.rights_explanation == "The rights explanation"

        transformation = hyperlink.resource.derived_through
        assert transformation.derivative == hyperlink.resource
        assert transformation.original == original
        assert transformation.settings.get("setting") == "a setting"
        assert [transformation] == original.transformations

    @pytest.mark.parametrize(
        'relation,default_filename',
        [
            pytest.param(Hyperlink.OPEN_ACCESS_DOWNLOAD, "content", id='content'),
            pytest.param(Hyperlink.IMAGE, "cover", id='cover'),
            pytest.param(Hyperlink.THUMBNAIL_IMAGE, "cover-thumbnail", id='cover-thumbnail'),
        ],
    )
    def test_default_filename(self, relation, default_filename):
        """
        GIVEN: A Hyperlink relation and a default filename
        WHEN:  Getting the relation's default filename
        THEN:  The default filename is correctly set
        """
        assert Hyperlink._default_filename(relation) == default_filename

    def test_unmirrored(self, db_session, create_identifier, create_work, default_library, init_datasource_and_genres):
        """
        GIVEN: Hyperlink resources, a Work, and a Collection
        WHEN:  Getting Hyperlinks associated with an item in a Collection that could be mirrored but aren't.
        THEN:  Returns resources that coudl be mirrored but aren't.
        """
        url = "www.example.com"
        ds = DataSource.lookup(db_session, DataSource.GUTENBERG)
        overdrive = DataSource.lookup(db_session, DataSource.OVERDRIVE)

        [collection] = default_library.collections
        collection.data_source = ds

        # Here's an Identifier associated with a collection.
        work = create_work(db_session, with_license_pool=True, collection=collection)
        [pool] = work.license_pools
        identifier1 = pool.identifier

        # This is a random identifier not associated with the collection.
        identifier2 = create_identifier(db_session)

        def unmirrored():
            return Hyperlink.unmirrored(collection).all()

        # Identifier is not in the collection.
        identifier2.add_link(Hyperlink.IMAGE, url+"/1", ds)
        assert unmirrored() == []

        # Hyperlink rel is not mirrorable.
        identifier1.add_link(
            "not mirrorable", url+"/2", ds, "text/plain"
        )
        assert unmirrored() == []

        # Hyperlink has no associated representation -- it needs to be
        # mirrored, which will create one!
        hyperlink, _ = identifier1.add_link(
            Hyperlink.IMAGE, url+"/3", ds, "image/png"
        )
        assert unmirrored() == [hyperlink]

        # Representation is already mirrored, so does not show up
        # in the unmirrored list.
        representation = hyperlink.resource.representation
        representation.set_as_mirrored(url+"/4")
        assert unmirrored() == []

        # Representation exists in database but is not mirrored -- it needs
        # to be mirrored!
        representation.mirror_url = None
        assert unmirrored() == [hyperlink]

        # Hyperlink is associated with a data source other than the
        # data source of the collection. It ought to be mirrored, but
        # this collection isn't responsible for mirroring it.
        hyperlink.data_source = overdrive
        assert unmirrored() == []


class TestResource:

    def test_as_delivery_mechanism_for(self, db_session, create_work):
        """
        GIVEN: A Work, a LicensePool, and a LicensePoolDeliveryMechanism
        WHEN:  Checking if the resource is used in a delivery mechanism for the given license pool
        THEN:  Returns the delivery mechanism if applicable
        """
        # Calling as_delivery_mechanism_for on a Resource that is used
        # to deliver a specific LicensePool returns the appropriate
        # LicensePoolDeliveryMechanism.
        work = create_work(db_session, with_open_access_download=True)
        [pool] = work.license_pools
        [lpdm] = pool.delivery_mechanisms
        assert lpdm == lpdm.resource.as_delivery_mechanism_for(pool)

        # If there's no relationship between the Resource and
        # the LicensePoolDeliveryMechanism, as_delivery_mechanism_for
        # returns None.
        w2 = create_work(db_session, with_license_pool=True)
        [unrelated] = w2.license_pools
        assert lpdm.resource.as_delivery_mechanism_for(unrelated) is None


class TestRepresentation:

    @pytest.mark.parametrize(
        'base,expected',
        [
            ("/foo/bar", "baz"),
            ("/foo/bar/", "baz"),
            ("/blah/blah/", "/foo/bar/baz")
        ]
    )
    def test_normalized_content_path(self, base, expected):
        """
        GIVEN: A content path
        WHEN:  Normalizing the content path with a base
        THEN:  Returns a string path with respect to the base
        """
        assert Representation.normalize_content_path("/foo/bar/baz", base) == expected

    @pytest.mark.parametrize(
        'url,headers,default,expected_headers_type',
        [
            # If there are no headers or no content-type header, the
            # presumed media type takes precedence.
            pytest.param("http://text/all.about.jpeg", None, "text/plain", "text/plain", id='no_headers'),
            pytest.param(None, {}, "text/plain", "text/plain", id='empty_headers'),
            # Most of the time, the content-type header takes precedence over the presumed media type.
            pytest.param(None, {"content-type": "image/gif"}, "text/plain", "image/gif", id='image/gif'),
            # Except when the content-type header is so generic as to be uselses.
            pytest.param(None, {"content-type": "application/octet-stream;profile=foo"},
                         "text/plain", "text/plain", id='generic_headers'),
            # If no default media type is specified, but one can be derived from
            # the URL, that one is used as the default.
            pytest.param("http://example.com/cover.jpeg", {"content-type": "application/octet-stream;profile=foo"},
                         None, "image/jpeg", id='no_default_media_type'),
            # But a default media type doesn't override a specific
            # Content-Type from the server, even if it superficially makes
            # more sense.
            pytest.param("http://images-galore/cover.jpeg", {"content-type": "image/png"},
                         None, "image/png", id='specific_content-type'),
        ],
    )
    def test_best_media_type(self, url, headers, default, expected_headers_type):
        """
        GIVEN: A URL, headers dict with a content-type, and a default content-type
        WHEN:  Determining whether the content-type header should override a presumed media type
        THEN:  Returns the most likely media type
        """
        assert Representation._best_media_type(url, headers, default) == expected_headers_type

    @pytest.mark.parametrize(
        'media_type,expected',
        [
            # Ebook formats and image formats get mirrored.
            (Representation.EPUB_MEDIA_TYPE, True),
            (Representation.MOBI_MEDIA_TYPE, True),
            (Representation.JPEG_MEDIA_TYPE, True),
            # Other media types don't get mirrored
            ("text/plain", False)
        ]
    )
    def test_mirrorable_media_type(self, db_session, create_representation, media_type, expected):
        """
        GIVEN: A media type
        WHEN:  Determining if the representation based on the media type is mirrorable
        THEN:  Returns True/False depending on the representation's media type
        """
        representation = create_representation(
            db_session, url="http://example.com", media_type=media_type, content="content")
        assert representation.mirrorable_media_type is expected

    def test_guess_media_type(self):
        """
        GIVEN: A path
        WHEN:  Guessing the media type from the path
        THEN:  A media type is returned
        """
        m_file = Representation.guess_media_type
        m_url = Representation.guess_url_media_type_from_path
        jpg_file = "file.jpg"
        zip_file = "file.ZIP"
        zip_file_rel_path = "relatively/pathed/file.zip"
        zip_file_abs_path = "/absolutely/pathed/file.zIp"

        assert Representation.JPEG_MEDIA_TYPE == m_file(jpg_file)
        assert Representation.ZIP_MEDIA_TYPE == m_file(zip_file)

        for extension, media_type in list(Representation.MEDIA_TYPE_FOR_EXTENSION.items()):
            filename = "file" + extension
            assert media_type == m_file(filename)

        assert m_file(None) is None
        assert m_file("file") is None
        assert m_file("file.unknown-extension") is None

        # URLs should be handled differently
        # Simple file-based guess will get this right, ...
        zip_url = "https://some_url/path/file.zip"
        assert Representation.ZIP_MEDIA_TYPE == m_file(zip_url)
        # ... but will get these wrong.
        zip_url_with_query = "https://some_url/path/file.zip?Policy=xyz123&Key-Pair-Id=xxx"
        zip_url_misleading = "https://some_url/path/file.zip?Policy=xyz123&associated_cover=image.jpg"
        assert m_file(zip_url_with_query) is None  # We get None, but want Zip
        assert Representation.JPEG_MEDIA_TYPE == m_file(zip_url_misleading)  # We get JPEG, but want Zip

        # Taking URL structure into account should get them all right.
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_url)
        #   ... but will get these wrong.
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_url_with_query)
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_url_misleading)

        # And we can handle local file cases
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_file)
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_file_rel_path)
        assert Representation.ZIP_MEDIA_TYPE == m_url(zip_file_abs_path)

    @pytest.mark.parametrize(
        'url,media_type,extension',
        [
            pytest.param('', 'text/unknown', '', id='unknown_file_at_/foo'),
            pytest.param('', 'text/plain', '.txt', id='text_file_at_/foo'),
            pytest.param('.jpg', 'image/jpeg', '.jpg', id='JPEG_at_/foo.jpg'),
            pytest.param('', 'image/jpeg', '.jpg', id='JPEG_at_/foo'),
            pytest.param('', 'image/png', '.png', id='PNG_at_/foo'),
            pytest.param('.epub.images', Representation.EPUB_MEDIA_TYPE, '.epub.images', id='EPUB_at_/foo.epub.images'),
            pytest.param('.svg', 'image/svg+xml', '.svg', id='SVG_at_/foo.svg'),
        ],
    )
    def test_external_media_type_and_extension(self, db_session, create_representation, url, media_type, extension):
        """
        GIVEN: A Representation
        WHEN:  Determining the external media type and extension
        THEN:  Returns a media type and returns an extension
        """
        """Test the various transformations that might happen to media type
        and extension when we mirror a representation.
        """
        url = "www.example.com/" + url
        representation = create_representation(db_session, url, media_type)
        assert representation.external_media_type == media_type
        assert representation.extension() == extension

    def test_set_fetched_content(self, db_session, create_representation):
        """
        GIVEN: A Representation
        WHEN:  Reading an open filehandle to the representation's contents
        THEN:  The representation's content is returned
        """
        representation = create_representation(db_session, "http://www.example.com/", "text/plain")
        representation.set_fetched_content("some text")
        assert representation.content_fh().read() == b"some text"

    def test_set_fetched_content_file_on_disk(self, db_session, create_representation, tmpdir):
        """
        GIVEN: A Representation that has content from a file on disk
        WHEN:  Reading an open filehandle to the representation's contents
        THEN:  The representation's content is returned
        """
        Configuration.instance[Configuration.DATA_DIRECTORY] = str(tmpdir)
        filename = "set_fetched_content_file_on_disk.txt"
        path = tmpdir.join(filename)
        path.write(b"some text")

        representation = create_representation(db_session, "http://www.example.com/", "text/plain")
        representation.set_fetched_content(None, str(path))
        fh = representation.content_fh()
        assert fh.read() == b"some text"

    def test_unicode_content_utf8_default(self, db_session, create_representation):
        """
        GIVEN: A Representation with unicode content
        WHEN:  Getting the representation's content and unicode_content
        THEN:  Representation's content is returned
        """
        unicode_content = "It’s complicated."

        utf8_content = unicode_content.encode("utf8")

        # This bytestring can be decoded as Windows-1252, but that
        # would be the wrong answer.
        bad_windows_1252 = utf8_content.decode("windows-1252")
        assert "Itâ€™s complicated." == bad_windows_1252

        representation = create_representation(db_session, "http://example.com/", "text/plain")
        representation.set_fetched_content(unicode_content, None)
        assert utf8_content == representation.content

        # By trying to interpret the content as UTF-8 before falling back to
        # Windows-1252, we get the right answer.
        assert unicode_content == representation.unicode_content

    def test_unicode_content_windows_1252(self, db_session, create_representation):
        """
        GIVEN: A Representation with unicode content encoded with windows-1252
        WHEN:  Getting the representation's content and unicode_content
        THEN:  Representation's content is returned
        """
        unicode_content = "A “love” story"
        windows_1252_content = unicode_content.encode("windows-1252")

        representation = create_representation(db_session, "http://example.com/", "text/plain")
        representation.set_fetched_content(windows_1252_content)
        assert windows_1252_content == representation.content
        assert unicode_content == representation.unicode_content

    def test_unicode_content_is_none_when_decoding_is_impossible(self, db_session, create_representation):
        """
        GIVEN: A Representation with byte content
        WHEN:  Getting the representatoin's content and unicode_content
        THEN:  Representation's content is returned and
               None is returned for unicode_content
        """
        byte_content = b"\x81\x02\x03"
        representation = create_representation(db_session, "http://example.com/", "text/plain")
        representation.set_fetched_content(byte_content)
        assert byte_content == representation.content
        assert representation.unicode_content is None

    @pytest.mark.parametrize(
        'media_type,expected_media_type',
        [
            # In the absence of a content-type header, the presumed_media_type takes over.
            (None, "text/xml"),
            # In the presence of a generic content-type header, the presumed_media_type takes over.
            ('application/octet-stream', "text/xml"),
            # A non-generic content-type header takes precedence over presumed_media_type.
            ("text/plain", "text/plain")
        ]
    )
    def test_presumed_media_type(self, db_session, media_type, expected_media_type):
        """
        GIVEN: A Representation
        WHEN:  Getting the representation and its media type
        THEN:  Returns the expected media type
        """
        client = DummyHTTPClient()
        client.queue_response(200, media_type, content='content')
        representation, _ = Representation.get(
            db_session, 'http://url', do_get=client.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        assert representation.media_type == expected_media_type

    def test_404_creates_cachable_representation(self, db_session):
        """
        GIVEN: A Representation
        WHEN:  Retrieving the Representation after a 404 response
        THEN:  Representation is retrieved from the database on the first call and from cache on the second call
        """
        h = DummyHTTPClient()
        h.queue_response(404)

        url = "http://example.com/"
        representation, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is False

        representation2, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is True
        assert representation2 == representation

    def test_302_creates_cachable_representation(self, db_session):
        """
        GIVEN: A Representation
        WHEN:  Retrieving the Representation after a 302 response
        THEN:  Representation is retrieved from the database on the first call and from cache on the second call
        """
        h = DummyHTTPClient()
        h.queue_response(302)

        url = "http://example.com/"
        representation, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is False

        representation2, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is True
        assert representation2 == representation

    def test_500_creates_uncachable_representation(self, db_session):
        """
        GIVEN: A Representation
        WHEN:  Retrieving the Representation after a 500 response
        THEN:  Representation is retrieved from the database
        """
        h = DummyHTTPClient()
        h.queue_response(500)
        url = "http://example.com/"

        _, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is False

        h.queue_response(500)
        _, cached = Representation.get(db_session, url, do_get=h.do_get)
        assert cached is False

    def test_response_reviewer_impacts_representation(self, db_session):
        """
        GIVEN: A Representation and a response reviewer
        WHEN:  Retrieving the Representation with a response viewer that raises an exception
        THEN:  The exception is captured in fetch_exception with the retrieved Representation
        """
        h = DummyHTTPClient()
        h.queue_response(200, media_type='text/html')
        url = "http://example.com"

        def reviewer(response):
            _, headers, _ = response
            if 'html' in headers['content-type']:
                raise Exception("No. Just no.")

        representation, cached = Representation.get(
            db_session, url, do_get=h.do_get, response_reviewer=reviewer
        )
        assert "No. Just no." in representation.fetch_exception
        assert cached is False

    def test_exception_handler(self, db_session):
        """
        GIVEN: A Representation and an exception handler
        WHEN:  Retrieving the Representation that raises an exception and re-raising it
        THEN:  Exception is re-raised instead of handled
        """
        url = "http://example.com/"

        def oops(*args, **kwargs):
            raise Exception("oops!")

        # By default exceptions raised during get() are
        # recorded along with the (empty) Representation objects
        representation, _ = Representation.get(db_session, url, do_get=oops)
        assert representation.fetch_exception.strip().endswith(
            "Exception: oops!"
        )
        assert representation.content is None
        assert representation.status_code is None

        # But we can ask that exceptions simply be re-raised instead of
        # being handled.
        with pytest.raises(Exception) as excinfo:
            Representation.get(
                db_session, url, do_get=oops,
                exception_handler=Representation.reraise_exception)
        assert "oops!" in str(excinfo.value)

    @pytest.mark.parametrize(
        'url,expected_extension',
        [
            ("test.epub", ".epub"),
            ("test.epub3", ".epub3"),
            ("test.epub.noimages", ".epub.noimages"),
            ("test.1234.abcd", ".abcd"),
            ("test", None),
            ("foo.com/", None),
            ("test.epub?version=3", ".epub")
        ]
    )
    def test_url_extension(self, db_session, create_representation, url, expected_extension):
        """
        GIVEN: A Representation with a URL
        WHEN:  Getting the file extension of the given Representation
        THEN:  A file extension is returned
        """
        representation = create_representation(db_session, url)
        assert representation.url_extension == expected_extension

    @pytest.mark.parametrize(
        'media_type,expected_media_type',
        [
            ("image/jpeg", "image/jpeg"),
            ("application/atom+xml;profile=opds-catalog;kind=acquisition", "application/atom+xml")
        ]
    )
    def test_clean_media_type(self, media_type, expected_media_type):
        """
        GIVEN: A media type
        WHEN:  Cleaning the media type
        THEN:  Media type is cleaned
        """
        assert Representation._clean_media_type(media_type) == expected_media_type

    @pytest.mark.parametrize(
        'media_type,expected_extension',
        [
            ("image/jpeg", ".jpg"),
            ("application/x-mobipocket-ebook", ".mobi"),
            ("no/such-media-type", "")
        ]
    )
    def test_extension(self, media_type, expected_extension):
        """
        GIVEN: A media type
        WHEN:  Getting the file extension for a given media type
        THEN:  A file extension is returned for the given media type
        """
        assert Representation._extension(media_type) == expected_extension

    @pytest.mark.parametrize(
        'url,link,destination_type,expected_filename',
        [
            # Here's a common sort of URL.
            # Here's the filename we would give it if we were to mirror it.
            ("http://example.com/foo/bar/baz.txt", None, None, "baz.txt"),
            # File extension is always set based on media type.
            ("http://example.com/foo/bar/baz.txt", None, "image/png", "baz.png"),
            # The original file extension is not treated as reliable and need not be present.
            ("http://example.com/1", None, "text/plain", "1.txt"),
            # Again, file extension is always set based on media type.
            ("http://example.com/1", None, "image/png", "1.png"),
            # In this case, we don't have an extension registered for text/unknown, so the extension is omitted.
            ("http://example.com/1", None, "text/unknown", "1"),
            # This URL has no path component, so we can't even come up with a
            # decent default filename. We have to go with 'resource'.
            ("http://example.com/", None, "text/unknown", "resource"),
            ("http://example.com/", None, "image/png", "resource.png"),
            # But if we know what type of thing we're linking to, we can do a little better.
            ("http://example.com/", Hyperlink(rel=Hyperlink.IMAGE), None, "cover"),
            ("http://example.com/", Hyperlink(rel=Hyperlink.IMAGE), "image/png", "cover.png"),
        ]
    )
    def test_default_filename(self, db_session, create_representation, url, link, destination_type, expected_filename):
        """
        GIVEN: A Representation located at a URL
        WHEN:  Determining a good filename for this representation
        THEN:  A filename is derived from the URL if possible otherwise it is named resoure
               and the file extension depends on the destination_type
        """
        representation = create_representation(db_session, url)
        assert representation.default_filename(link=link, destination_type=destination_type) == expected_filename

    def test_cautious_http_get(self):
        """
        GIVEN: A resource at a URL
        WHEN:  Cautiously making a GET request to a domain that is in the do not access list
        THEN:  GET request is not performed
        """
        h = DummyHTTPClient()
        h.queue_response(200, content="yay")

        # If the domain is obviously safe, the GET request goes through,
        # with no HEAD request being made.
        status, headers, content = Representation.cautious_http_get(
            "http://safe.org/", {}, do_not_access=['unsafe.org'],
            do_get=h.do_get, cautious_head_client=object()
        )
        assert status == 200
        assert content == b"yay"

        # If the domain is obviously unsafe, no GET request or HEAD
        # request is made.
        status, headers, content = Representation.cautious_http_get(
            "http://unsafe.org/", {}, do_not_access=['unsafe.org'],
            do_get=object(), cautious_head_client=object()
        )
        assert status == 417
        assert content == "Cautiously decided not to make a GET request to http://unsafe.org/"

        # If the domain is potentially unsafe, a HEAD request is made,
        # and the answer depends on its outcome.

        # Here, the HEAD request redirects to a prohibited site.
        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://unsafe.org/")
            )
        status, headers, content = Representation.cautious_http_get(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=object(), cautious_head_client=mock_redirect
        )
        assert status == 417
        assert headers['content-type'] == "application/vnd.librarysimplified-did-not-make-request"
        assert content == "Cautiously decided not to make a GET request to http://caution.org/"

        # Here, the HEAD request redirects to an allowed site.
        h.queue_response(200, content="good content")

        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://safe.org/")
            )
        status, headers, content = Representation.cautious_http_get(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=h.do_get, cautious_head_client=mock_redirect
        )
        assert status == 200
        assert content == b"good content"

    def bad_redirect(*args, **kwargs):
        return MockRequestsResponse(
            301, dict(
                location="http://www.gutenberg.org/a-book.html"
            )
        )

    def good_redirect(*args, **kwargs):
        return MockRequestsResponse(
            301,
            dict(location="http://www.some-other-site.org/a-book.epub")
        )

    def not_a_redirect(*args, **kwargs):
        return MockRequestsResponse(200)

    @pytest.mark.parametrize(
        'url,headers,do_not_access,check_for_redirect,head_client,useful',
        [
            # If get_would_be_useful tries to use this object to make a HEAD request, the test will blow up.
            # Most sites are safe with no HEAD request necessary.
            ("http://www.safe-site.org/book.epub", {}, None, None, object(), True),
            # gutenberg.org is problematic, no HEAD request necessary.
            ("http://www.gutenberg.org/book.epub", {}, None, None, object(), False),
            # do_not_access controls which domains should always be considered unsafe.
            ("http://www.safe-site.org/book.epub", {}, ['safe-site.org'], None, object(), False),
            ("http://www.gutenberg.org/book.epub", {}, ['safe-site.org'], None, object(), True),
            # Domain match is based on a subdomain match, not a substring match.
            ("http://www.not-unsafe-site.org/book.epub", {}, ['unsafe-site.org'], None, object(), True),
            # Some domains (unglue.it) are known to make surprise redirects to unsafe domains.
            # For these, we must make a HEAD request to check.
            ("http://www.unglue.it/book", {}, None, None, bad_redirect, False),
            ("http://www.unglue.it/book", {}, None, None, good_redirect, True),
            ("http://www.unglue.it/book", {}, None, None, not_a_redirect, True),
            # The `check_for_redirect` argument controls which domains are checked using HEAD requests.
            # Here, we customise it to check a site other than unglue.it.
            ("http://www.questionable-site.org/book.epub", {}, None, ['questionable-site.org'], bad_redirect, False),
        ]
    )
    def test_get_would_be_useful(self, url, headers, do_not_access, check_for_redirect, head_client, useful):
        """
        GIVEN: A resouce at a URL
        WHEN:  Determining whether a GET request will go (or redirect) to a site we don't to make requests to.
        THEN:  The resource location is determined to be useful or not
        """
        would_be_useful = Representation.get_would_be_useful(
            url=url,
            headers=headers,
            do_not_access=do_not_access,
            check_for_redirect=check_for_redirect,
            head_client=head_client
        )
        assert would_be_useful is useful

    def test_get_with_url_normalizer(self, db_session):
        """
        GIVEN: A resource at a URL
        WHEN:  Verifying our ability to store a Resource under a URL other than
               the exact URL used to make the HTTP request
        THEN:  Resource is stored under a different URL than the one used to make the HTTP request
        """
        class Normalizer(object):
            called_with = None

            def normalize(self, url):
                # Strip off a session ID from an outgoing URL.
                self.called_with = url
                return url[:11]

        normalizer = Normalizer()

        h = DummyHTTPClient()
        h.queue_response(200, content="yay")
        original_url = "http://url/?sid=12345"

        representation, from_cache = Representation.get(
            db_session, original_url, do_get=h.do_get,
            url_normalizer=normalizer.normalize
        )

        # The original URL was used to make the actual request.
        assert [original_url] == h.requests

        # The original URL was then passed into Normalizer.normalize
        assert original_url == normalizer.called_with

        # And the normalized URL was used as the Representation's
        # storage key.
        normalized_url = "http://url/"
        assert "yay" == representation.content.decode("utf-8")
        assert normalized_url == representation.url
        assert from_cache is False

        # Try again, and the Representation is retrieved from cache under
        # the normalized URL.
        #
        # Replace do_get with a dud object to prove that no second
        # request goes out 'over the wire'.
        representation2, from_cache = Representation.get(
            db_session, original_url, do_get=object(),
            url_normalizer=normalizer.normalize
        )
        assert from_cache is True
        assert representation2 == representation
        assert normalized_url == representation.url

    def test_best_thumbnail(self, db_session, create_representation):
        """
        GIVEN: Representations with thumbnails
        WHEN:  Getting the best thumbnail
        THEN:  Thumbnails that are mirrored are preferred
        """
        # This Representation has no thumbnails.
        representation = create_representation(db_session)
        assert representation.best_thumbnail is None

        # Now it has two thumbnails, neither of which is mirrored.
        t1 = create_representation(db_session)
        t2 = create_representation(db_session)
        for i in t1, t2:
            representation.thumbnails.append(i)

        # There's no distinction between the thumbnails, so the first one
        # is selected as 'best'.
        assert t1 == representation.best_thumbnail

        # If one of the thumbnails is mirrored, it becomes the 'best'
        # thumbnail.
        t2.set_as_mirrored("http://example.com/")
        assert t2 == representation.best_thumbnail


class TestCoverResource:

    def test_set_cover(self, db_session, create_edition, create_representation, get_sample_cover_path):
        """
        GIVEN: An Edition, a LicensePool, and a cover iamge
        WHEN:  Setting the Edition's cover image
        THEN:  Cover image is set
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        original = "http://original"
        mirror = "http://mirror"
        thumbnail_mirror = "http://thumbnail_mirror"
        sample_cover_path = get_sample_cover_path("test-book-cover.png")
        hyperlink, _ = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            content=open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        assert edition.cover_full_url == mirror
        assert edition.cover_thumbnail_url is None

        # Now scale the cover.
        thumbnail = create_representation(db_session)
        thumbnail.thumbnail_of = full_rep
        thumbnail.set_as_mirrored(thumbnail_mirror)
        edition.set_cover(hyperlink.resource)
        assert edition.cover_full_url == mirror
        assert edition.cover_thumbnail_url == thumbnail_mirror

    def test_set_cover_for_very_small_image(self, db_session, create_edition, get_sample_cover_path):
        """
        GIVEN: An Edition, a LicensePool, and a cover image
        WHEN:  Setting the Edition's cover image
        THEN:  Cover image is set
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        original = "http://original"
        mirror = "http://mirror"
        sample_cover_path = get_sample_cover_path("tiny-image-cover.png")
        hyperlink, _ = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        assert edition.cover_full_url == mirror
        assert edition.cover_thumbnail_url == mirror

    def test_set_cover_for_smallish_image_uses_full_sized_image_as_thumbnail(
            self, db_session, create_edition, get_sample_cover_path):
        """
        GIVEN: An Edition, a LicensePool, and a cover image
        WHEN:  Setting the Edition's cover image with a small image
        THEN:  Thumbnail uses full sized image
        """
        edition, pool = create_edition(db_session, with_license_pool=True)
        original = "http://original"
        mirror = "http://mirror"
        sample_cover_path = get_sample_cover_path("tiny-image-cover.png")
        hyperlink, _ = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        # For purposes of this test, pretend that the full-sized image is
        # larger than a thumbnail, but not terribly large.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT

        edition.set_cover(hyperlink.resource)
        assert edition.cover_full_url == mirror
        assert edition.cover_thumbnail_url == mirror

        # If the full-sized image had been slightly larger, we would have
        # decided not to use a thumbnail at all.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT + 1
        edition.cover_thumbnail_url = None
        edition.set_cover(hyperlink.resource)
        assert edition.cover_thumbnail_url is None

    def test_attempt_to_scale_non_image_sets_scale_exception(self, db_session, create_representation):
        """
        GIVEN: A Representation with a media type of "text/plain"
        WHEN:  Scaling the representation
        THEN:  ValueError is raised due to the media type
        """
        rep = create_representation(db_session, media_type="text/plain", content="foo")
        scaled, _ = rep.scale(300, 600, "http://example.com", "image/png")
        expect = "ValueError: Cannot load non-image representation as image: type text/plain"
        assert scaled == rep
        assert expect in rep.scale_exception

    def test_cannot_scale_to_non_image(self, db_session, create_representation):
        """
        GIVEN: A Representation
        WHEN:  Scaling the image with a destination media type of "text/plain"
        THEN:  A ValueError is raised for an unsupported destination media type
        """
        rep = create_representation(db_session, media_type="image/png", content="foo")
        with pytest.raises(ValueError) as excinfo:
            rep.scale(300, 600, "http://example.com", "text/plain")
        assert "Unsupported destination media type: text/plain" in str(excinfo.value)

    def test_success(self, get_sample_cover_representation):
        """
        GIVEN: A Representation for a cover image
        WHEN:  Forcefully scaling a cover image
        THEN:  Image is scaled
        """
        cover = get_sample_cover_representation("test-book-cover.png")
        url = "http://example.com/"
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        assert is_new is True
        assert thumbnail.url == url
        assert thumbnail.mirror_url is None
        assert thumbnail.mirrored_at is None
        assert thumbnail.thumbnail_of == cover
        assert thumbnail.media_type == "image/png"
        assert thumbnail.image_height == 300
        assert thumbnail.image_width == 200

        # Try to scale the image to the same URL, and nothing will
        # happen, even though the proposed image size is
        # different.
        thumbnail2, is_new = cover.scale(400, 700, url, "image/png")
        assert thumbnail2 == thumbnail
        assert is_new is False

        # Let's say the thumbnail has been mirrored.
        thumbnail.set_as_mirrored("http://mirrored")

        old_content = thumbnail.content
        # With the force argument we can forcibly re-scale an image,
        # changing its size.
        assert [thumbnail] == cover.thumbnails
        thumbnail2, is_new = cover.scale(400, 700, url, "image/png", force=True)
        assert is_new is True
        assert [thumbnail2] == cover.thumbnails
        assert thumbnail2.thumbnail_of == cover

        # The same Representation, but now its data is different.
        assert thumbnail == thumbnail2
        assert thumbnail2.content != old_content
        assert thumbnail.image_height == 400
        # The width turns out to be 266 or 267 depending on the machine.
        assert 265 < thumbnail.image_width < 268

        # The thumbnail has been regenerated, so it needs to be mirrored again.
        assert thumbnail.mirrored_at is None

    def test_book_with_odd_aspect_ratio(self, get_sample_cover_representation):
        """
        GIVEN: A Representation for a cover iamge
        WHEN:  Scaling the cover iamge
        THEN:  Image is scaled
        """
        # This book is 1200x600.
        cover = get_sample_cover_representation("childrens-book-cover.png")
        url = "http://example.com/"
        thumbnail, is_new = cover.scale(300, 400, url, "image/png")
        assert is_new is True
        assert thumbnail.url == url
        assert thumbnail.thumbnail_of == cover
        # The width was reduced to max_width, a reduction of a factor of three
        assert thumbnail.image_width == 400
        # The height was also reduced by a factory of three, even
        # though this takes it below max_height.
        assert thumbnail.image_height == 200

    def test_book_smaller_than_thumbnail_size(self, get_sample_cover_representation):
        """
        GIVEN: A Representation for a cover image
        WHEN:  Scaling a thumbnail larger than the cover image
        THEN:  Thumbnail is not created
        """
        # This book is 200x200. No thumbnail will be created.
        cover = get_sample_cover_representation("tiny-image-cover.png")
        url = "http://example.com/"
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        assert is_new is False
        assert thumbnail == cover
        assert cover.thumbnails == []
        assert thumbnail.thumbnail_of is None
        assert thumbnail.url != url

    def test_image_type_priority(self):
        """
        GIVEN: Image types
        WHEN:  Determining image type priority
        THEN:  Image type is correctly chosen
        """
        # All else being equal, we prefer some image types over
        # others. Better image types get lower numbers.
        m = Resource.image_type_priority
        assert m(None) is None
        assert m(Representation.EPUB_MEDIA_TYPE) is None

        png = m(Representation.PNG_MEDIA_TYPE)
        jpeg = m(Representation.JPEG_MEDIA_TYPE)
        gif = m(Representation.GIF_MEDIA_TYPE)
        svg = m(Representation.SVG_MEDIA_TYPE)

        assert png < jpeg
        assert jpeg < gif
        assert gif < svg

    def test_best_covers_among(
            self, db_session, create_edition, get_sample_cover_representation, init_datasource_and_genres):
        """
        GIVEN: Multiple cover image Representations
        WHEN:  Choosing the best cover from these representations
        THEN:  The best cover image is chosen
        """
        # Here's a book with a thumbnail image.
        _, pool = create_edition(db_session, with_license_pool=True)

        link1, _ = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://example.com/1", pool.data_source
        )
        resource_with_no_representation = link1.resource

        # A resource with no representation is not considered even if
        # it's the only option.
        assert [] == Resource.best_covers_among([resource_with_no_representation])

        # Here's an abysmally bad cover.
        lousy_cover = get_sample_cover_representation("tiny-image-cover.png")
        lousy_cover.image_height = 1
        lousy_cover.image_width = 10000
        link2, _ = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://example.com/2", pool.data_source
        )
        resource_with_lousy_cover = link2.resource
        resource_with_lousy_cover.representation = lousy_cover

        # This cover is so bad that it's not even considered if it's
        # the only option.
        assert [] == Resource.best_covers_among([resource_with_lousy_cover])

        # Here's a decent cover.
        decent_cover = get_sample_cover_representation("test-book-cover.png")
        link3, _ = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://example.com/3", pool.data_source
        )
        resource_with_decent_cover = link3.resource
        resource_with_decent_cover.representation = decent_cover

        # This cover is at least good enough to pass muster if there
        # is no other option.
        assert (
            [resource_with_decent_cover] ==
            Resource.best_covers_among([resource_with_decent_cover]))

        # Let's create another cover image with identical
        # characteristics.
        link4, _ = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://example.com/4", pool.data_source
        )
        decent_cover_2 = get_sample_cover_representation("test-book-cover.png")
        resource_with_decent_cover_2 = link4.resource
        resource_with_decent_cover_2.representation = decent_cover_2

        covers = [resource_with_decent_cover, resource_with_decent_cover_2]

        # best_covers_among() can't decide between the two -- they have
        # the same score.
        assert set(covers) == set(Resource.best_covers_among(covers))

        # All else being equal, if one cover is an PNG and the other
        # is a JPEG, we prefer the PNG.
        resource_with_decent_cover.representation.media_type = Representation.JPEG_MEDIA_TYPE
        assert [resource_with_decent_cover_2] == Resource.best_covers_among(covers)

        # But if the metadata wrangler said to use the JPEG, we use the JPEG.
        metadata_wrangler = DataSource.lookup(
            db_session, DataSource.METADATA_WRANGLER
        )
        resource_with_decent_cover.data_source = metadata_wrangler

        # ...the decision becomes easy.
        assert [resource_with_decent_cover] == Resource.best_covers_among(covers)

    def test_rejection_and_approval(self, db_session, create_edition, get_sample_cover_representation):
        """
        GIVEN: Image Representations for a cover and thumbnail
        WHEN:  Approving or rejecting the image resource
        THEN:  Resource quality adjusts accordingly
        """
        # Create a Resource.
        _, pool = create_edition(db_session, with_open_access_download=True)
        link = pool.add_link(Hyperlink.IMAGE, "http://example.com/", pool.data_source)[0]
        cover = link.resource

        # Give it all the right covers.
        cover_rep = get_sample_cover_representation("test-book-cover.png")
        thumbnail_rep = get_sample_cover_representation("test-book-cover.png")
        cover.representation = cover_rep
        cover_rep.thumbnails.append(thumbnail_rep)

        # Set its quality.
        cover.quality_as_thumbnail_image
        original_quality = cover.quality
        assert True == (original_quality > 0)

        # Rejecting it sets the voted_quality and quality below zero.
        cover.reject()
        assert True == (cover.voted_quality < 0)
        assert True == (cover.quality < 0)

        # If the quality is already below zero, rejecting it doesn't
        # change the value.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        assert True == (last_votes_for_quality > 0)
        cover.reject()
        assert last_voted_quality == cover.voted_quality
        assert last_votes_for_quality == cover.votes_for_quality
        assert last_quality == cover.quality

        # If the quality is approved, the votes are updated as expected.
        cover.approve()
        assert 0 == cover.voted_quality
        assert 2 == cover.votes_for_quality
        # Because the number of human votes have gone up in contention,
        # the overall quality is lower than it was originally.
        assert True == (cover.quality < original_quality)
        # But it's still above zero.
        assert True == (cover.quality > 0)

        # Approving the cover again improves its quality further.
        last_quality = cover.quality
        cover.approve()
        assert True == (cover.voted_quality > 0)
        assert 3 == cover.votes_for_quality
        assert True == (cover.quality > last_quality)

        # Rejecting the cover again will make the existing value negative.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        cover.reject()
        assert -last_voted_quality == cover.voted_quality
        assert True == (cover.quality < 0)

        assert last_votes_for_quality+1 == cover.votes_for_quality

    def test_quality_as_thumbnail_image(
            self, db_session, create_edition, get_sample_cover_representation, init_datasource_and_genres):
        """
        GIVEN: A Representation for a cover image
        WHEN:  Determining the image's suitability as a thumbnail image
        THEN:  A numerical quality is calculated
        """
        # Get some data sources ready, since a big part of image
        # quality comes from data source.
        gutenberg = DataSource.lookup(db_session, DataSource.GUTENBERG)
        gutenberg_cover_generator = DataSource.lookup(
            db_session, DataSource.GUTENBERG_COVER_GENERATOR
        )
        overdrive = DataSource.lookup(db_session, DataSource.OVERDRIVE)
        metadata_wrangler = DataSource.lookup(
            db_session, DataSource.METADATA_WRANGLER
        )

        # Here's a book with a thumbnail image.
        _, pool = create_edition(db_session, with_license_pool=True)
        hyperlink, _ = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, "http://example.com/", overdrive
        )
        resource = hyperlink.resource

        # Without a representation, the thumbnail image is useless.
        assert 0 == resource.quality_as_thumbnail_image

        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        cover = get_sample_cover_representation("tiny-image-cover.png")
        resource.representation = cover
        assert 1.0 == resource.quality_as_thumbnail_image

        # Changing the image aspect ratio affects the quality as per
        # thumbnail_size_quality_penalty.
        cover.image_height = ideal_height * 2
        cover.image_width = ideal_width
        assert 0.5 == resource.quality_as_thumbnail_image

        # Changing the data source also affects the quality. Gutenberg
        # covers are penalized heavily...
        cover.image_height = ideal_height
        cover.image_width = ideal_width
        resource.data_source = gutenberg
        assert 0.5 == resource.quality_as_thumbnail_image

        # The Gutenberg cover generator is penalized less heavily.
        resource.data_source = gutenberg_cover_generator
        assert 0.6 == resource.quality_as_thumbnail_image

        # The metadata wrangler actually gets a _bonus_, to encourage the
        # use of its covers over those provided by license sources.
        resource.data_source = metadata_wrangler
        assert 2 == resource.quality_as_thumbnail_image

    def test_thumbnail_size_quality_penalty(self):
        """
        GIVEN: Dimensions of an image
        WHEN:  Determining the quality penalty of the dimensions
        THEN:  A quotient is calculated
        """
        # Verify that Representation._cover_size_quality_penalty penalizes
        # images that are the wrong aspect ratio, or too small.
        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        quality_penalty = Representation._thumbnail_size_quality_penalty

        # In the absence of any size information we assume
        # everything's fine.
        assert quality_penalty(None, None) == 1

        # The perfect image has no penalty.
        assert quality_penalty(ideal_width, ideal_height) == 1

        # An image that is the perfect aspect ratio, but too large,
        # has no penalty.
        assert quality_penalty(ideal_width*2, ideal_height*2) == 1

        # An image that is the perfect aspect ratio, but is too small,
        # is penalised.
        assert quality_penalty(ideal_width*0.5, ideal_height*0.5) == 1/4.0
        assert quality_penalty(ideal_width*0.25, ideal_height*0.25) == 1/16.0

        # An image that deviates from the perfect aspect ratio is
        # penalized in proportion.
        assert quality_penalty(ideal_width*2, ideal_height) == 1/2.0
        assert quality_penalty(ideal_width, ideal_height*2) == 1/2.0
        assert quality_penalty(ideal_width*4, ideal_height) == 1/4.0
        assert quality_penalty(ideal_width, ideal_height*4) == 1/4.0
