# encoding: utf-8
from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)
import os
from .. import (
    DatabaseTest,
    DummyHTTPClient,
)
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

class TestHyperlink(DatabaseTest):

    def test_add_link(self):
        edition, pool = self._edition(with_license_pool=True)
        identifier = edition.primary_identifier
        data_source = pool.data_source
        original, ignore = create(self._db, Resource, url="http://bar.com")
        hyperlink, is_new = pool.add_link(
            Hyperlink.DESCRIPTION, "http://foo.com/", data_source,
            "text/plain", "The content", None, RightsStatus.CC_BY,
            "The rights explanation", original,
            transformation_settings=dict(setting="a setting"))
        eq_(True, is_new)
        rep = hyperlink.resource.representation
        eq_("text/plain", rep.media_type)
        eq_(b"The content", rep.content)
        eq_(Hyperlink.DESCRIPTION, hyperlink.rel)
        eq_(identifier, hyperlink.identifier)
        eq_(RightsStatus.CC_BY, hyperlink.resource.rights_status.uri)
        eq_("The rights explanation", hyperlink.resource.rights_explanation)
        transformation = hyperlink.resource.derived_through
        eq_(hyperlink.resource, transformation.derivative)
        eq_(original, transformation.original)
        eq_("a setting", transformation.settings.get("setting"))
        eq_([transformation], original.transformations)

    def test_default_filename(self):
        m = Hyperlink._default_filename
        eq_("content", m(Hyperlink.OPEN_ACCESS_DOWNLOAD))
        eq_("cover", m(Hyperlink.IMAGE))
        eq_("cover-thumbnail", m(Hyperlink.THUMBNAIL_IMAGE))

    def test_unmirrored(self):

        ds = DataSource.lookup(self._db, DataSource.GUTENBERG)
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        c1 = self._default_collection
        c1.data_source = ds

        # Here's an Identifier associated with a collection.
        work = self._work(with_license_pool=True, collection=c1)
        [pool] = work.license_pools
        i1 = pool.identifier

        # This is a random identifier not associated with the collection.
        i2 = self._identifier()

        def m():
            return Hyperlink.unmirrored(c1).all()

        # Identifier is not in the collection.
        not_in_collection, ignore = i2.add_link(Hyperlink.IMAGE, self._url, ds)
        eq_([], m())

        # Hyperlink rel is not mirrorable.
        wrong_type, ignore = i1.add_link(
            "not mirrorable", self._url, ds, "text/plain"
        )
        eq_([], m())

        # Hyperlink has no associated representation -- it needs to be
        # mirrored, which will create one!
        hyperlink, ignore = i1.add_link(
            Hyperlink.IMAGE, self._url, ds, "image/png"
        )
        eq_([hyperlink], m())

        # Representation is already mirrored, so does not show up
        # in the unmirrored list.
        representation = hyperlink.resource.representation
        representation.set_as_mirrored(self._url)
        eq_([], m())

        # Representation exists in database but is not mirrored -- it needs
        # to be mirrored!
        representation.mirror_url = None
        eq_([hyperlink], m())

        # Hyperlink is associated with a data source other than the
        # data source of the collection. It ought to be mirrored, but
        # this collection isn't responsible for mirroring it.
        hyperlink.data_source = overdrive
        eq_([], m())


class TestResource(DatabaseTest):

    def test_as_delivery_mechanism_for(self):

        # Calling as_delivery_mechanism_for on a Resource that is used
        # to deliver a specific LicensePool returns the appropriate
        # LicensePoolDeliveryMechanism.
        work = self._work(with_open_access_download=True)
        [pool] = work.license_pools
        [lpdm] = pool.delivery_mechanisms
        eq_(lpdm, lpdm.resource.as_delivery_mechanism_for(pool))

        # If there's no relationship between the Resource and
        # the LicensePoolDeliveryMechanism, as_delivery_mechanism_for
        # returns None.
        w2 = self._work(with_license_pool=True)
        [unrelated] = w2.license_pools
        eq_(None, lpdm.resource.as_delivery_mechanism_for(unrelated))


class TestRepresentation(DatabaseTest):

    def test_normalized_content_path(self):
        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar"))

        eq_("baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/foo/bar/"))

        eq_("/foo/bar/baz", Representation.normalize_content_path(
            "/foo/bar/baz", "/blah/blah/"))

    def test_best_media_type(self):
        """Test our ability to determine whether the Content-Type
        header should override a presumed media type.
        """
        m = Representation._best_media_type

        # If there are no headers or no content-type header, the
        # presumed media type takes precedence.
        eq_("text/plain", m("http://text/all.about.jpeg", None, "text/plain"))
        eq_("text/plain", m(None, {}, "text/plain"))

        # Most of the time, the content-type header takes precedence over
        # the presumed media type.
        eq_("image/gif", m(None, {"content-type": "image/gif"}, "text/plain"))

        # Except when the content-type header is so generic as to be uselses.
        eq_("text/plain", m(
            None,
            {"content-type": "application/octet-stream;profile=foo"},
            "text/plain")
        )

        # If no default media type is specified, but one can be derived from
        # the URL, that one is used as the default.
        eq_("image/jpeg", m(
            "http://images-galore/cover.jpeg",
            {"content-type": "application/octet-stream;profile=foo"},
            None)
        )

        # But a default media type doesn't override a specific
        # Content-Type from the server, even if it superficially makes
        # more sense.
        eq_("image/png", m(
            "http://images-galore/cover.jpeg",
            {"content-type": "image/png"},
            None)
        )


    def test_mirrorable_media_type(self):
        representation, ignore = self._representation(self._url)

        # Ebook formats and image formats get mirrored.
        representation.media_type = Representation.EPUB_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)
        representation.media_type = Representation.MOBI_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)
        representation.media_type = Representation.JPEG_MEDIA_TYPE
        eq_(True, representation.mirrorable_media_type)

        # Other media types don't get mirrored
        representation.media_type = "text/plain"
        eq_(False, representation.mirrorable_media_type)

    def test_guess_media_type(self):
        m_file = Representation.guess_media_type
        m_url = Representation.guess_url_media_type_from_path
        jpg_file = "file.jpg"
        zip_file = "file.ZIP"
        zip_file_rel_path = "relatively/pathed/file.zip"
        zip_file_abs_path = "/absolutely/pathed/file.zIp"

        eq_(Representation.JPEG_MEDIA_TYPE, m_file(jpg_file))
        eq_(Representation.ZIP_MEDIA_TYPE, m_file(zip_file))

        for extension, media_type in Representation.MEDIA_TYPE_FOR_EXTENSION.items():
            filename = "file" + extension
            eq_(media_type, m_file(filename))

        eq_(None, m_file(None))
        eq_(None, m_file("file"))
        eq_(None, m_file("file.unknown-extension"))

        # URLs should be handled differently
        # Simple file-based guess will get this right, ...
        zip_url = "https://some_url/path/file.zip"
        eq_(Representation.ZIP_MEDIA_TYPE, m_file(zip_url))
        # ... but will get these wrong.
        zip_url_with_query = "https://some_url/path/file.zip?Policy=xyz123&Key-Pair-Id=xxx"
        zip_url_misleading = "https://some_url/path/file.zip?Policy=xyz123&associated_cover=image.jpg"
        eq_(None, m_file(zip_url_with_query))  # We get None, but want Zip
        eq_(Representation.JPEG_MEDIA_TYPE, m_file(zip_url_misleading))  # We get JPEG, but want Zip

        # Taking URL structure into account should get them all right.
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_url))
        #   ... but will get these wrong.
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_url_with_query))
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_url_misleading))

        # And we can handle local file cases
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_file))
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_file_rel_path))
        eq_(Representation.ZIP_MEDIA_TYPE, m_url(zip_file_abs_path))



    def test_external_media_type_and_extension(self):
        """Test the various transformations that might happen to media type
        and extension when we mirror a representation.
        """

        # An unknown file at /foo
        representation, ignore = self._representation(self._url, "text/unknown")
        eq_("text/unknown", representation.external_media_type)
        eq_('', representation.extension())

        # A text file at /foo
        representation, ignore = self._representation(self._url, "text/plain")
        eq_("text/plain", representation.external_media_type)
        eq_('.txt', representation.extension())

        # A JPEG at /foo.jpg
        representation, ignore = self._representation(
            self._url + ".jpg", "image/jpeg"
        )
        eq_("image/jpeg", representation.external_media_type)
        eq_(".jpg", representation.extension())

        # A JPEG at /foo
        representation, ignore = self._representation(self._url, "image/jpeg")
        eq_("image/jpeg", representation.external_media_type)
        eq_(".jpg", representation.extension())

        # A PNG at /foo
        representation, ignore = self._representation(self._url, "image/png")
        eq_("image/png", representation.external_media_type)
        eq_(".png", representation.extension())

        # An EPUB at /foo.epub.images -- information present in the URL
        # is preserved.
        representation, ignore = self._representation(
            self._url + '.epub.images', Representation.EPUB_MEDIA_TYPE
        )
        eq_(Representation.EPUB_MEDIA_TYPE, representation.external_media_type)
        eq_(".epub.images", representation.extension())

        representation, ignore = self._representation(self._url + ".svg", "image/svg+xml")
        eq_("image/svg+xml", representation.external_media_type)
        eq_(".svg", representation.extension())

    def test_set_fetched_content(self):
        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content("some text")
        eq_(b"some text", representation.content_fh().read())

    def test_set_fetched_content_file_on_disk(self):
        filename = "set_fetched_content_file_on_disk.txt"
        path = os.path.join(self.tmp_data_dir, filename)
        open(path, "wb").write(b"some text")

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(None, filename)
        fh = representation.content_fh()
        eq_(b"some text", fh.read())

    def test_unicode_content_utf8_default(self):
        unicode_content = u"It’s complicated."

        utf8_content = unicode_content.encode("utf8")

        # This bytestring can be decoded as Windows-1252, but that
        # would be the wrong answer.
        bad_windows_1252 = utf8_content.decode("windows-1252")
        eq_(u"Itâ€™s complicated.", bad_windows_1252)

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(unicode_content, None)
        eq_(utf8_content, representation.content)

        # By trying to interpret the content as UTF-8 before falling back to
        # Windows-1252, we get the right answer.
        eq_(unicode_content, representation.unicode_content)

    def test_unicode_content_windows_1252(self):
        unicode_content = u"A “love” story"
        windows_1252_content = unicode_content.encode("windows-1252")

        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(windows_1252_content)
        eq_(windows_1252_content, representation.content)
        eq_(unicode_content, representation.unicode_content)

    def test_unicode_content_is_none_when_decoding_is_impossible(self):
        byte_content = b"\x81\x02\x03"
        representation, ignore = self._representation(self._url, "text/plain")
        representation.set_fetched_content(byte_content)
        eq_(byte_content, representation.content)
        eq_(None, representation.unicode_content)

    def test_presumed_media_type(self):
        h = DummyHTTPClient()

        # In the absence of a content-type header, the presumed_media_type
        # takes over.
        h.queue_response(200, None, content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/xml', representation.media_type)

        # In the presence of a generic content-type header, the
        # presumed_media_type takes over.
        h.queue_response(200, 'application/octet-stream',
                         content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/xml', representation.media_type)

        # A non-generic content-type header takes precedence over
        # presumed_media_type.
        h.queue_response(200, 'text/plain', content='content')
        representation, cached = Representation.get(
            self._db, 'http://url', do_get=h.do_get, max_age=0,
            presumed_media_type="text/xml"
        )
        eq_('text/plain', representation.media_type)


    def test_404_creates_cachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(404)

        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        representation2, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(True, cached)
        eq_(representation, representation2)

    def test_302_creates_cachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(302)

        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        representation2, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(True, cached)
        eq_(representation, representation2)

    def test_500_creates_uncachable_representation(self):
        h = DummyHTTPClient()
        h.queue_response(500)
        url = self._url
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

        h.queue_response(500)
        representation, cached = Representation.get(
            self._db, url, do_get=h.do_get)
        eq_(False, cached)

    def test_response_reviewer_impacts_representation(self):
        h = DummyHTTPClient()
        h.queue_response(200, media_type='text/html')

        def reviewer(response):
            status, headers, content = response
            if 'html' in headers['content-type']:
                raise Exception("No. Just no.")

        representation, cached = Representation.get(
            self._db, self._url, do_get=h.do_get, response_reviewer=reviewer
        )
        assert "No. Just no." in representation.fetch_exception
        eq_(False, cached)

    def test_exception_handler(self):
        def oops(*args, **kwargs):
            raise Exception("oops!")

        # By default exceptions raised during get() are
        # recorded along with the (empty) Representation objects
        representation, cached = Representation.get(
            self._db, self._url, do_get=oops,
        )
        assert representation.fetch_exception.strip().endswith(
            "Exception: oops!"
        )
        eq_(None, representation.content)
        eq_(None, representation.status_code)

        # But we can ask that exceptions simply be re-raised instead of
        # being handled.
        assert_raises_regexp(
            Exception, "oops!", Representation.get,
            self._db, self._url, do_get=oops,
            exception_handler=Representation.reraise_exception
        )

    def test_url_extension(self):
        epub, ignore = self._representation("test.epub")
        eq_(".epub", epub.url_extension)

        epub3, ignore = self._representation("test.epub3")
        eq_(".epub3", epub3.url_extension)

        noimages, ignore = self._representation("test.epub.noimages")
        eq_(".epub.noimages", noimages.url_extension)

        unknown, ignore = self._representation("test.1234.abcd")
        eq_(".abcd", unknown.url_extension)

        no_extension, ignore = self._representation("test")
        eq_(None, no_extension.url_extension)

        no_filename, ignore = self._representation("foo.com/")
        eq_(None, no_filename.url_extension)

        query_param, ignore = self._representation("test.epub?version=3")
        eq_(".epub", query_param.url_extension)

    def test_clean_media_type(self):
        m = Representation._clean_media_type
        eq_("image/jpeg", m("image/jpeg"))
        eq_("application/atom+xml",
            m("application/atom+xml;profile=opds-catalog;kind=acquisition")
        )

    def test_extension(self):
        m = Representation._extension
        eq_(".jpg", m("image/jpeg"))
        eq_(".mobi", m("application/x-mobipocket-ebook"))
        eq_("", m("no/such-media-type"))

    def test_default_filename(self):

        # Here's a common sort of URL.
        url = "http://example.com/foo/bar/baz.txt"
        representation, ignore = self._representation(url)

        # Here's the filename we would give it if we were to mirror
        # it.
        filename = representation.default_filename()
        eq_("baz.txt", filename)

        # File extension is always set based on media type.
        filename = representation.default_filename(destination_type="image/png")
        eq_("baz.png", filename)

        # The original file extension is not treated as reliable and
        # need not be present.
        url = "http://example.com/1"
        representation, ignore = self._representation(url, "text/plain")
        filename = representation.default_filename()
        eq_("1.txt", filename)

        # Again, file extension is always set based on media type.
        filename = representation.default_filename(destination_type="image/png")
        eq_("1.png", filename)

        # In this case, we don't have an extension registered for
        # text/unknown, so the extension is omitted.
        filename = representation.default_filename(destination_type="text/unknown")
        eq_("1", filename)

        # This URL has no path component, so we can't even come up with a
        # decent default filename. We have to go with 'resource'.
        representation, ignore = self._representation("http://example.com/", "text/unknown")
        eq_('resource', representation.default_filename())
        eq_('resource.png', representation.default_filename(destination_type="image/png"))

        # But if we know what type of thing we're linking to, we can
        # do a little better.
        link = Hyperlink(rel=Hyperlink.IMAGE)
        filename = representation.default_filename(link=link)
        eq_('cover', filename)
        filename = representation.default_filename(link=link, destination_type="image/png")
        eq_('cover.png', filename)

    def test_cautious_http_get(self):

        h = DummyHTTPClient()
        h.queue_response(200, content="yay")

        # If the domain is obviously safe, the GET request goes through,
        # with no HEAD request being made.
        m = Representation.cautious_http_get
        status, headers, content = m(
            "http://safe.org/", {}, do_not_access=['unsafe.org'],
            do_get=h.do_get, cautious_head_client=object()
        )
        eq_(200, status)
        eq_("yay", content)

        # If the domain is obviously unsafe, no GET request or HEAD
        # request is made.
        status, headers, content = m(
            "http://unsafe.org/", {}, do_not_access=['unsafe.org'],
            do_get=object(), cautious_head_client=object()
        )
        eq_(417, status)
        eq_("Cautiously decided not to make a GET request to http://unsafe.org/",
            content)

        # If the domain is potentially unsafe, a HEAD request is made,
        # and the answer depends on its outcome.

        # Here, the HEAD request redirects to a prohibited site.
        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://unsafe.org/")
            )
        status, headers, content = m(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=object(), cautious_head_client=mock_redirect
        )
        eq_(417, status)
        eq_("application/vnd.librarysimplified-did-not-make-request",
            headers['content-type'])
        eq_("Cautiously decided not to make a GET request to http://caution.org/",
            content)

        # Here, the HEAD request redirects to an allowed site.
        h.queue_response(200, content="good content")
        def mock_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(location="http://safe.org/")
            )
        status, headers, content = m(
            "http://caution.org/", {},
            do_not_access=['unsafe.org'],
            check_for_redirect=['caution.org'],
            do_get=h.do_get, cautious_head_client=mock_redirect
        )
        eq_(200, status)
        eq_("good content", content)

    def test_get_would_be_useful(self):
        """Test the method that determines whether a GET request will go (or
        redirect) to a site we don't to make requests to.
        """
        safe = Representation.get_would_be_useful

        # If get_would_be_useful tries to use this object to make a HEAD
        # request, the test will blow up.
        fake_head = object()

        # Most sites are safe with no HEAD request necessary.
        eq_(True, safe("http://www.safe-site.org/book.epub", {},
                       head_client=fake_head))

        # gutenberg.org is problematic, no HEAD request necessary.
        eq_(False, safe("http://www.gutenberg.org/book.epub", {},
                        head_client=fake_head))

        # do_not_access controls which domains should always be
        # considered unsafe.
        eq_(
            False, safe(
                "http://www.safe-site.org/book.epub", {},
                do_not_access=['safe-site.org'], head_client=fake_head
            )
        )
        eq_(
            True, safe(
                "http://www.gutenberg.org/book.epub", {},
                do_not_access=['safe-site.org'], head_client=fake_head
            )
        )

        # Domain match is based on a subdomain match, not a substring
        # match.
        eq_(True, safe("http://www.not-unsafe-site.org/book.epub", {},
                       do_not_access=['unsafe-site.org'],
                       head_client=fake_head))

        # Some domains (unglue.it) are known to make surprise
        # redirects to unsafe domains. For these, we must make a HEAD
        # request to check.

        def bad_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301, dict(
                    location="http://www.gutenberg.org/a-book.html"
                )
            )
        eq_(False, safe("http://www.unglue.it/book", {},
                        head_client=bad_redirect))

        def good_redirect(*args, **kwargs):
            return MockRequestsResponse(
                301,
                dict(location="http://www.some-other-site.org/a-book.epub")
            )
        eq_(
            True,
            safe("http://www.unglue.it/book", {}, head_client=good_redirect)
        )

        def not_a_redirect(*args, **kwargs):
            return MockRequestsResponse(200)
        eq_(True, safe("http://www.unglue.it/book", {},
                       head_client=not_a_redirect))

        # The `check_for_redirect` argument controls which domains are
        # checked using HEAD requests. Here, we customise it to check
        # a site other than unglue.it.
        eq_(False, safe("http://www.questionable-site.org/book.epub", {},
                        check_for_redirect=['questionable-site.org'],
                        head_client=bad_redirect))

    def test_get_with_url_normalizer(self):
        # Verify our ability to store a Resource under a URL other than
        # the exact URL used to make the HTTP request.

        class Normalizer(object):
            called_with = None
            def normalize(self, url):
                # Strip off a  session ID from an outgoing URL.
                self.called_with = url
                return url[:11]

        normalizer = Normalizer()

        h = DummyHTTPClient()
        h.queue_response(200, content="yay")
        original_url = "http://url/?sid=12345"

        representation, from_cache = Representation.get(
            self._db, original_url, do_get=h.do_get,
            url_normalizer=normalizer.normalize
        )

        # The original URL was used to make the actual request.
        eq_([original_url], h.requests)

        # The original URL was then passed into Normalizer.normalize
        eq_(original_url, normalizer.called_with)

        # And the normalized URL was used as the Representation's
        # storage key.
        normalized_url = "http://url/"
        eq_("yay", representation.content)
        eq_(normalized_url, representation.url)
        eq_(False, from_cache)

        # Try again, and the Representation is retrieved from cache under
        # the normalized URL.
        #
        # Replace do_get with a dud object to prove that no second
        # request goes out 'over the wire'.
        representation2, from_cache = Representation.get(
            self._db, original_url, do_get=object(),
            url_normalizer=normalizer.normalize
        )
        eq_(True, from_cache)
        eq_(representation2, representation)
        eq_(normalized_url, representation.url)

    def test_best_thumbnail(self):
        # This Representation has no thumbnails.
        representation, ignore = self._representation()
        eq_(None, representation.best_thumbnail)

        # Now it has two thumbnails, neither of which is mirrored.
        t1, ignore = self._representation()
        t2, ignore = self._representation()
        for i in t1, t2:
            representation.thumbnails.append(i)

        # There's no distinction between the thumbnails, so the first one
        # is selected as 'best'.
        eq_(t1, representation.best_thumbnail)

        # If one of the thumbnails is mirrored, it becomes the 'best'
        # thumbnail.
        t2.set_as_mirrored(self._url)
        eq_(t2, representation.best_thumbnail)

class TestCoverResource(DatabaseTest):

    def test_set_cover(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        thumbnail_mirror = self._url
        sample_cover_path = self.sample_cover_path("test-book-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            content=open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(None, edition.cover_thumbnail_url)

        # Now scale the cover.
        thumbnail, ignore = self._representation()
        thumbnail.thumbnail_of = full_rep
        thumbnail.set_as_mirrored(thumbnail_mirror)
        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(thumbnail_mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_very_small_image(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

    def test_set_cover_for_smallish_image_uses_full_sized_image_as_thumbnail(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url
        mirror = self._url
        sample_cover_path = self.sample_cover_path("tiny-image-cover.png")
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source, "image/png",
            open(sample_cover_path, 'rb').read()
        )
        full_rep = hyperlink.resource.representation
        full_rep.set_as_mirrored(mirror)

        # For purposes of this test, pretend that the full-sized image is
        # larger than a thumbnail, but not terribly large.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT

        edition.set_cover(hyperlink.resource)
        eq_(mirror, edition.cover_full_url)
        eq_(mirror, edition.cover_thumbnail_url)

        # If the full-sized image had been slightly larger, we would have
        # decided not to use a thumbnail at all.
        hyperlink.resource.representation.image_height = Edition.MAX_FALLBACK_THUMBNAIL_HEIGHT + 1
        edition.cover_thumbnail_url = None
        edition.set_cover(hyperlink.resource)
        eq_(None, edition.cover_thumbnail_url)


    def test_attempt_to_scale_non_image_sets_scale_exception(self):
        rep, ignore = self._representation(media_type="text/plain", content="foo")
        scaled, ignore = rep.scale(300, 600, self._url, "image/png")
        expect = "ValueError: Cannot load non-image representation as image: type text/plain"
        assert scaled == rep
        assert expect in rep.scale_exception

    def test_cannot_scale_to_non_image(self):
        rep, ignore = self._representation(media_type="image/png", content="foo")
        assert_raises_regexp(
            ValueError,
            "Unsupported destination media type: text/plain",
            rep.scale, 300, 600, self._url, "text/plain")

    def test_success(self):
        cover = self.sample_cover_representation("test-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(None, thumbnail.mirror_url)
        eq_(None, thumbnail.mirrored_at)
        eq_(cover, thumbnail.thumbnail_of)
        eq_("image/png", thumbnail.media_type)
        eq_(300, thumbnail.image_height)
        eq_(200, thumbnail.image_width)

        # Try to scale the image to the same URL, and nothing will
        # happen, even though the proposed image size is
        # different.
        thumbnail2, is_new = cover.scale(400, 700, url, "image/png")
        eq_(thumbnail2, thumbnail)
        eq_(False, is_new)

        # Let's say the thumbnail has been mirrored.
        thumbnail.set_as_mirrored(self._url)

        old_content = thumbnail.content
        # With the force argument we can forcibly re-scale an image,
        # changing its size.
        eq_([thumbnail], cover.thumbnails)
        thumbnail2, is_new = cover.scale(
            400, 700, url, "image/png", force=True)
        eq_(True, is_new)
        eq_([thumbnail2], cover.thumbnails)
        eq_(cover, thumbnail2.thumbnail_of)

        # The same Representation, but now its data is different.
        eq_(thumbnail, thumbnail2)
        assert thumbnail2.content != old_content
        eq_(400, thumbnail.image_height)
        eq_(266, thumbnail.image_width)

        # The thumbnail has been regenerated, so it needs to be mirrored again.
        eq_(None, thumbnail.mirrored_at)

    def test_book_with_odd_aspect_ratio(self):
        # This book is 1200x600.
        cover = self.sample_cover_representation("childrens-book-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 400, url, "image/png")
        eq_(True, is_new)
        eq_(url, thumbnail.url)
        eq_(cover, thumbnail.thumbnail_of)
        # The width was reduced to max_width, a reduction of a factor of three
        eq_(400, thumbnail.image_width)
        # The height was also reduced by a factory of three, even
        # though this takes it below max_height.
        eq_(200, thumbnail.image_height)

    def test_book_smaller_than_thumbnail_size(self):
        # This book is 200x200. No thumbnail will be created.
        cover = self.sample_cover_representation("tiny-image-cover.png")
        url = self._url
        thumbnail, is_new = cover.scale(300, 600, url, "image/png")
        eq_(False, is_new)
        eq_(thumbnail, cover)
        eq_([], cover.thumbnails)
        eq_(None, thumbnail.thumbnail_of)
        assert thumbnail.url != url

    def test_image_type_priority(self):
        """Test the image_type_priority method.

        All else being equal, we prefer some image types over
        others. Better image types get lower numbers.
        """
        m = Resource.image_type_priority
        eq_(None, m(None))
        eq_(None, m(Representation.EPUB_MEDIA_TYPE))

        png = m(Representation.PNG_MEDIA_TYPE)
        jpeg = m(Representation.JPEG_MEDIA_TYPE)
        gif = m(Representation.GIF_MEDIA_TYPE)
        svg = m(Representation.SVG_MEDIA_TYPE)

        assert png < jpeg
        assert jpeg < gif
        assert gif < svg

    def test_best_covers_among(self):
        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)

        link1, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_no_representation = link1.resource

        # A resource with no representation is not considered even if
        # it's the only option.
        eq_([], Resource.best_covers_among([resource_with_no_representation]))

        # Here's an abysmally bad cover.
        lousy_cover = self.sample_cover_representation("tiny-image-cover.png")
        lousy_cover.image_height=1
        lousy_cover.image_width=10000
        link2, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_lousy_cover = link2.resource
        resource_with_lousy_cover.representation = lousy_cover

        # This cover is so bad that it's not even considered if it's
        # the only option.
        eq_([], Resource.best_covers_among([resource_with_lousy_cover]))

        # Here's a decent cover.
        decent_cover = self.sample_cover_representation("test-book-cover.png")
        link3, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        resource_with_decent_cover = link3.resource
        resource_with_decent_cover.representation = decent_cover

        # This cover is at least good enough to pass muster if there
        # is no other option.
        eq_(
            [resource_with_decent_cover],
            Resource.best_covers_among([resource_with_decent_cover])
        )

        # Let's create another cover image with identical
        # characteristics.
        link4, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, pool.data_source
        )
        decent_cover_2 = self.sample_cover_representation("test-book-cover.png")
        resource_with_decent_cover_2 = link4.resource
        resource_with_decent_cover_2.representation = decent_cover_2

        l = [resource_with_decent_cover, resource_with_decent_cover_2]

        # best_covers_among() can't decide between the two -- they have
        # the same score.
        eq_(set(l), set(Resource.best_covers_among(l)))

        # All else being equal, if one cover is an PNG and the other
        # is a JPEG, we prefer the PNG.
        resource_with_decent_cover.representation.media_type = Representation.JPEG_MEDIA_TYPE
        eq_([resource_with_decent_cover_2], Resource.best_covers_among(l))

        # But if the metadata wrangler said to use the JPEG, we use the JPEG.
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )
        resource_with_decent_cover.data_source = metadata_wrangler

        # ...the decision becomes easy.
        eq_([resource_with_decent_cover], Resource.best_covers_among(l))

    def test_rejection_and_approval(self):
        # Create a Resource.
        edition, pool = self._edition(with_open_access_download=True)
        link = pool.add_link(Hyperlink.IMAGE, self._url, pool.data_source)[0]
        cover = link.resource

        # Give it all the right covers.
        cover_rep = self.sample_cover_representation("test-book-cover.png")
        thumbnail_rep = self.sample_cover_representation("test-book-cover.png")
        cover.representation = cover_rep
        cover_rep.thumbnails.append(thumbnail_rep)

        # Set its quality.
        cover.quality_as_thumbnail_image
        original_quality = cover.quality
        eq_(True, original_quality > 0)

        # Rejecting it sets the voted_quality and quality below zero.
        cover.reject()
        eq_(True, cover.voted_quality < 0)
        eq_(True, cover.quality < 0)

        # If the quality is already below zero, rejecting it doesn't
        # change the value.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        eq_(True, last_votes_for_quality > 0)
        cover.reject()
        eq_(last_voted_quality, cover.voted_quality)
        eq_(last_votes_for_quality, cover.votes_for_quality)
        eq_(last_quality, cover.quality)

        # If the quality is approved, the votes are updated as expected.
        cover.approve()
        eq_(0, cover.voted_quality)
        eq_(2, cover.votes_for_quality)
        # Because the number of human votes have gone up in contention,
        # the overall quality is lower than it was originally.
        eq_(True, cover.quality < original_quality)
        # But it's still above zero.
        eq_(True, cover.quality > 0)

        # Approving the cover again improves its quality further.
        last_quality = cover.quality
        cover.approve()
        eq_(True, cover.voted_quality > 0)
        eq_(3, cover.votes_for_quality)
        eq_(True, cover.quality > last_quality)

        # Rejecting the cover again will make the existing value negative.
        last_voted_quality = cover.voted_quality
        last_votes_for_quality = cover.votes_for_quality
        last_quality = cover.quality
        cover.reject()
        eq_(-last_voted_quality, cover.voted_quality)
        eq_(True, cover.quality < 0)

        eq_(last_votes_for_quality+1, cover.votes_for_quality)

    def test_quality_as_thumbnail_image(self):

        # Get some data sources ready, since a big part of image
        # quality comes from data source.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        gutenberg_cover_generator = DataSource.lookup(
            self._db, DataSource.GUTENBERG_COVER_GENERATOR
        )
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        metadata_wrangler = DataSource.lookup(
            self._db, DataSource.METADATA_WRANGLER
        )

        # Here's a book with a thumbnail image.
        edition, pool = self._edition(with_license_pool=True)
        hyperlink, ignore = pool.add_link(
            Hyperlink.THUMBNAIL_IMAGE, self._url, overdrive
        )
        resource = hyperlink.resource

        # Without a representation, the thumbnail image is useless.
        eq_(0, resource.quality_as_thumbnail_image)

        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        cover = self.sample_cover_representation("tiny-image-cover.png")
        resource.representation = cover
        eq_(1.0, resource.quality_as_thumbnail_image)

        # Changing the image aspect ratio affects the quality as per
        # thumbnail_size_quality_penalty.
        cover.image_height = ideal_height * 2
        cover.image_width = ideal_width
        eq_(0.5, resource.quality_as_thumbnail_image)

        # Changing the data source also affects the quality. Gutenberg
        # covers are penalized heavily...
        cover.image_height = ideal_height
        cover.image_width = ideal_width
        resource.data_source = gutenberg
        eq_(0.5, resource.quality_as_thumbnail_image)

        # The Gutenberg cover generator is penalized less heavily.
        resource.data_source = gutenberg_cover_generator
        eq_(0.6, resource.quality_as_thumbnail_image)

        # The metadata wrangler actually gets a _bonus_, to encourage the
        # use of its covers over those provided by license sources.
        resource.data_source = metadata_wrangler
        eq_(2, resource.quality_as_thumbnail_image)

    def test_thumbnail_size_quality_penalty(self):
        """Verify that Representation._cover_size_quality_penalty penalizes
        images that are the wrong aspect ratio, or too small.
        """

        ideal_ratio = Identifier.IDEAL_COVER_ASPECT_RATIO
        ideal_height = Identifier.IDEAL_IMAGE_HEIGHT
        ideal_width = Identifier.IDEAL_IMAGE_WIDTH

        def f(width, height):
            return Representation._thumbnail_size_quality_penalty(width, height)

        # In the absence of any size information we assume
        # everything's fine.
        eq_(1, f(None, None))

        # The perfect image has no penalty.
        eq_(1, f(ideal_width, ideal_height))

        # An image that is the perfect aspect ratio, but too large,
        # has no penalty.
        eq_(1, f(ideal_width*2, ideal_height*2))

        # An image that is the perfect aspect ratio, but is too small,
        # is penalised.
        eq_(1/4.0, f(ideal_width*0.5, ideal_height*0.5))
        eq_(1/16.0, f(ideal_width*0.25, ideal_height*0.25))

        # An image that deviates from the perfect aspect ratio is
        # penalized in proportion.
        eq_(1/2.0, f(ideal_width*2, ideal_height))
        eq_(1/2.0, f(ideal_width, ideal_height*2))
        eq_(1/4.0, f(ideal_width*4, ideal_height))
        eq_(1/4.0, f(ideal_width, ideal_height*4))
