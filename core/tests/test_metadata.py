import csv
import datetime
import os
from copy import deepcopy
import pytest
from parameterized import parameterized

from ..testing import (
    DatabaseTest,
    DummyHTTPClient,
    DummyMetadataClient,
)
from ..analytics import Analytics
from ..classifier import Classifier
from ..classifier import NO_VALUE, NO_NUMBER
from ..metadata_layer import (
    CSVMetadataImporter,
    CirculationData,
    ContributorData,
    IdentifierData,
    LinkData,
    MARCExtractor,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
    TimestampData,
)
from ..model import (
    Contributor,
    CoverageRecord,
    DataSource,
    Edition,
    Identifier,
    Measurement,
    Hyperlink,
    Representation,
    RightsStatus,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
)
from ..model.configuration import ExternalIntegrationLink
from ..s3 import MockS3Uploader
from ..util.http import RemoteIntegrationException
from ..util.datetime_helpers import (
    datetime_utc,
    strptime_utc,
    to_utc,
    utc_now,
)


class TestIdentifierData(object):

    def test_constructor(self):
        data = IdentifierData(Identifier.ISBN, "foo", 0.5)
        assert Identifier.ISBN == data.type
        assert "foo" == data.identifier
        assert 0.5 == data.weight


class TestMetadataImporter(DatabaseTest):
    def test_parse(self):
        base_path = os.path.split(__file__)[0]
        path = os.path.join(
            base_path, "files/csv/staff_picks_small.csv")
        reader = csv.DictReader(open(path))
        importer = CSVMetadataImporter(
            DataSource.LIBRARY_STAFF,
        )
        generator = importer.to_metadata(reader)
        m1, m2, m3 = list(generator)

        assert "Horrorst\xf6r" == m1.title
        assert "Grady Hendrix" == m1.contributors[0].display_name
        assert "Martin Jensen" == m2.contributors[0].display_name

        # Let's check out the identifiers we found.

        # The first book has an Overdrive ID
        [overdrive] = m1.identifiers
        assert Identifier.OVERDRIVE_ID == overdrive.type
        assert '504BA8F6-FF4E-4B57-896E-F1A50CFFCA0C' == overdrive.identifier
        assert 0.75 == overdrive.weight

        # The second book has no ID at all.
        assert [] == m2.identifiers

        # The third book has both a 3M ID and an Overdrive ID.
        overdrive, threem = sorted(m3.identifiers, key=lambda x: x.identifier)

        assert Identifier.OVERDRIVE_ID == overdrive.type
        assert 'eae60d41-e0b8-4f9d-90b5-cbc43d433c2f' == overdrive.identifier
        assert 0.75 == overdrive.weight

        assert Identifier.THREEM_ID == threem.type
        assert 'eswhyz9' == threem.identifier
        assert 0.75 == threem.weight

        # Now let's check out subjects.
        assert (
            [
                ('schema:typicalAgeRange', 'Adult', 100),
                ('tag', 'Character Driven', 100),
                ('tag', 'Historical', 100),
                ('tag', 'Nail-Biters', 100),
                ('tag', 'Setting Driven', 100)
            ] ==
            [(x.type, x.identifier, x.weight)
             for x in sorted(m2.subjects, key=lambda x: x.identifier)])

    def test_classifications_from_another_source_not_updated(self):

        # Set up an edition whose primary identifier has two
        # classifications.
        source1 = DataSource.lookup(self._db, DataSource.AXIS_360)
        source2 = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        edition = self._edition()
        identifier = edition.primary_identifier
        c1 = identifier.classify(source1, Subject.TAG, "i will persist")
        c2 = identifier.classify(source2, Subject.TAG, "i will perish")

        # Now we get some new metadata from source #2.
        subjects = [SubjectData(type=Subject.TAG, identifier="i will conquer")]
        metadata = Metadata(subjects=subjects, data_source=source2)
        replace = ReplacementPolicy(subjects=True)
        metadata.apply(edition, None, replace=replace)

        # The old classification from source #2 has been destroyed.
        # The old classification from source #1 is still there.
        assert (
            ['i will conquer', 'i will persist'] ==
            sorted([x.subject.identifier for x in identifier.classifications]))

    def test_links(self):
        edition = self._edition()
        l1 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        l2 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        metadata = Metadata(links=[l1, l2],
                            data_source=edition.data_source)
        metadata.apply(edition, None)
        [image, description] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )
        assert Hyperlink.IMAGE == image.rel
        assert "http://example.com/" == image.resource.url

        assert Hyperlink.DESCRIPTION == description.rel
        assert b"foo" == description.resource.representation.content

    def test_image_with_original_and_rights(self):
        edition = self._edition()
        data_source = DataSource.lookup(self._db, DataSource.LIBRARY_STAFF)
        original = LinkData(rel=Hyperlink.IMAGE,
                            href="http://example.com/",
                            media_type=Representation.PNG_MEDIA_TYPE,
                            rights_uri=RightsStatus.PUBLIC_DOMAIN_USA,
                            rights_explanation="This image is from 1922",
                            )
        image_data = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x01\x03\x00\x00\x00%\xdbV\xca\x00\x00\x00\x06PLTE\xffM\x00\x01\x01\x01\x8e\x1e\xe5\x1b\x00\x00\x00\x01tRNS\xcc\xd24V\xfd\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'
        derivative = LinkData(rel=Hyperlink.IMAGE,
                              href="generic uri",
                              content=image_data,
                              media_type=Representation.PNG_MEDIA_TYPE,
                              rights_uri=RightsStatus.PUBLIC_DOMAIN_USA,
                              rights_explanation="This image is from 1922",
                              original=original,
                              transformation_settings=dict(position='top')
                              )

        metadata = Metadata(links=[derivative], data_source=data_source)
        metadata.apply(edition, None)
        [image] = edition.primary_identifier.links
        assert Hyperlink.IMAGE == image.rel
        assert "generic uri" == image.resource.url
        assert image_data == image.resource.representation.content
        assert RightsStatus.PUBLIC_DOMAIN_USA == image.resource.rights_status.uri
        assert "This image is from 1922" == image.resource.rights_explanation

        assert [] == image.resource.transformations
        transformation = image.resource.derived_through
        assert image.resource == transformation.derivative

        assert "http://example.com/" == transformation.original.url
        assert RightsStatus.PUBLIC_DOMAIN_USA == transformation.original.rights_status.uri
        assert "This image is from 1922" == transformation.original.rights_explanation
        assert "top" == transformation.settings.get("position")

    def test_image_and_thumbnail(self):
        edition = self._edition()
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://thumbnail.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        l1 = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/", thumbnail=l2,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        # Even though we're only passing in the primary image link...
        metadata = Metadata(links=[l1],
                            data_source=edition.data_source)
        metadata.apply(edition, None)

        # ...a Hyperlink is also created for the thumbnail.
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )
        assert Hyperlink.IMAGE == image.rel
        assert ([thumbnail.resource.representation] ==
            image.resource.representation.thumbnails)

    def test_thumbnail_isnt_a_thumbnail(self):
        edition = self._edition()
        not_a_thumbnail = LinkData(
            rel=Hyperlink.DESCRIPTION, content="A great book",
            media_type=Representation.TEXT_PLAIN,
        )
        image = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/",
            thumbnail=not_a_thumbnail,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )

        metadata = Metadata(links=[image],
                            data_source=edition.data_source)
        metadata.apply(edition, None)

        # Only one Hyperlink was created for the image, because
        # the alleged 'thumbnail' wasn't actually a thumbnail.
        [image_obj] = edition.primary_identifier.links
        assert Hyperlink.IMAGE == image_obj.rel
        assert [] == image_obj.resource.representation.thumbnails

        # If we pass in the 'thumbnail' separately, a Hyperlink is
        # created for it, but it's still not a thumbnail of anything.
        metadata = Metadata(links=[image, not_a_thumbnail],
                            data_source=edition.data_source)
        metadata.apply(edition, None)
        [image, description] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )
        assert Hyperlink.DESCRIPTION == description.rel
        assert b"A great book" == description.resource.representation.content
        assert [] == image.resource.representation.thumbnails
        assert None == description.resource.representation.thumbnail_of

    def test_image_and_thumbnail_are_the_same(self):
        edition = self._edition()
        url = "http://tinyimage.com/image.jpg"
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href=url,
        )
        l1 = LinkData(
            rel=Hyperlink.IMAGE, href=url, thumbnail=l2,
        )
        metadata = Metadata(links=[l1, l2],
                            data_source=edition.data_source)
        metadata.apply(edition, None)
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )

        # The image and its thumbnail point to the same resource.
        assert image.resource == thumbnail.resource

        assert Hyperlink.IMAGE == image.rel
        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel

        # The thumbnail is marked as a thumbnail of the main image.
        assert ([thumbnail.resource.representation] ==
            image.resource.representation.thumbnails)
        assert url == edition.cover_full_url
        assert url == edition.cover_thumbnail_url

    def test_image_becomes_representation_but_thumbnail_does_not(self):
        edition = self._edition()

        # The thumbnail link has no media type, and none can be
        # derived from the URL.
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://tinyimage.com/",
        )

        # The full-sized image link does not have this problem.
        l1 = LinkData(
            rel=Hyperlink.IMAGE, href="http://largeimage.com/", thumbnail=l2,
            media_type=Representation.JPEG_MEDIA_TYPE,
        )
        metadata = Metadata(links=[l1],
                            data_source=edition.data_source)
        metadata.apply(edition, None)

        # Both LinkData objects have been imported as Hyperlinks.
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )

        # However, since no Representation was created for the thumbnail,
        # the relationship between the main image and the thumbnail could
        # not be imported.
        assert None == thumbnail.resource.representation
        assert [] == image.resource.representation.thumbnails

        # The edition ends up with a full-sized image but no
        # thumbnail. This could potentially be improved, since we know
        # the two Resources are associated with the same Identifier.
        # But we lose track of the fact that the two Resources are
        # _the same image_ at different resolutions.
        assert "http://largeimage.com/" == edition.cover_full_url
        assert None == edition.cover_thumbnail_url

    def sample_cover_path(self, name):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "covers")
        sample_cover_path = os.path.join(resource_path, name)
        return sample_cover_path

    def test_image_scale_and_mirror(self):
        # Make sure that open access material links are translated to our S3 buckets, and that
        # commercial material links are left as is.
        # Note: mirroring links is now also CirculationData's job.  So the unit tests
        # that test for that have been changed to call to mirror cover images.
        # However, updated tests passing does not guarantee that all code now
        # correctly calls on CirculationData, too.  This is a risk.

        mirrors = dict(covers_mirror=MockS3Uploader(),books_mirror=None)
        edition, pool = self._edition(with_license_pool=True)
        content = open(self.sample_cover_path("test-book-cover.png"), "rb").read()
        l1 = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/",
            media_type=Representation.JPEG_MEDIA_TYPE,
            content=content
        )
        thumbnail_content = open(self.sample_cover_path("tiny-image-cover.png"), "rb").read()
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://example.com/thumb.jpg",
            media_type=Representation.JPEG_MEDIA_TYPE,
            content=content
        )

        # When we call metadata.apply, all image links will be scaled and
        # 'mirrored'.
        policy = ReplacementPolicy(mirrors=mirrors)
        metadata = Metadata(links=[l1, l2], data_source=edition.data_source)
        metadata.apply(edition, pool.collection, replace=policy)

        # Two Representations were 'mirrored'.
        image, thumbnail = mirrors[ExternalIntegrationLink.COVERS].uploaded

        # The image...
        [image_link] = image.resource.links
        assert Hyperlink.IMAGE == image_link.rel

        # And its thumbnail.
        assert image == thumbnail.thumbnail_of

        # The original image is too big to be a thumbnail.
        assert 600 == image.image_height
        assert 400 == image.image_width

        # The thumbnail is the right height.
        assert Edition.MAX_THUMBNAIL_HEIGHT == thumbnail.image_height
        assert Edition.MAX_THUMBNAIL_WIDTH == thumbnail.image_width

        # The thumbnail is newly generated from the full-size
        # image--the thumbnail that came in from the OPDS feed was
        # ignored.
        assert thumbnail.url != l2.href
        assert thumbnail.content != l2.content

        # Both images have been 'mirrored' to Amazon S3.
        assert image.mirror_url.startswith('https://test-cover-bucket.s3.amazonaws.com/')
        assert image.mirror_url.endswith('cover.jpg')

        # The thumbnail image has been converted to PNG.
        assert thumbnail.mirror_url.startswith('https://test-cover-bucket.s3.amazonaws.com/scaled/300/')
        assert thumbnail.mirror_url.endswith('cover.png')

    def test_mirror_thumbnail_only(self):
        # Make sure a thumbnail image is mirrored when there's no cover image.
        mirrors = dict(covers_mirror=MockS3Uploader())
        mirror_type = ExternalIntegrationLink.COVERS
        edition, pool = self._edition(with_license_pool=True)
        thumbnail_content = open(self.sample_cover_path("tiny-image-cover.png"), "rb").read()
        l = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://example.com/thumb.png",
            media_type=Representation.PNG_MEDIA_TYPE,
            content=thumbnail_content
        )

        policy = ReplacementPolicy(mirrors=mirrors)
        metadata = Metadata(links=[l], data_source=edition.data_source)
        metadata.apply(edition, pool.collection, replace=policy)

        # One Representation was 'mirrored'.
        [thumbnail] = mirrors[mirror_type].uploaded

        # The image has been 'mirrored' to Amazon S3.
        assert thumbnail.mirror_url.startswith('https://test-cover-bucket.s3.amazonaws.com/')
        assert thumbnail.mirror_url.endswith('thumb.png')

    def test_mirror_open_access_link_fetch_failure(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirrors = dict(covers_mirror=MockS3Uploader())
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        link = LinkData(
            rel=Hyperlink.IMAGE,
            media_type=Representation.JPEG_MEDIA_TYPE,
            href="http://example.com/",
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )
        h.queue_response(403)

        m.mirror_link(edition, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # Fetch failed, so we should have a fetch exception but no mirror url.
        assert representation.fetch_exception != None
        assert None == representation.mirror_exception
        assert None == representation.mirror_url
        assert link.href == representation.url
        assert representation.fetched_at != None
        assert None == representation.mirrored_at

        # the edition's identifier-associated license pool should not be
        # suppressed just because fetch failed on getting image.
        assert False == pool.suppressed

        # the license pool only gets its license_exception column filled in
        # if fetch failed on getting an Hyperlink.OPEN_ACCESS_DOWNLOAD-type epub.
        assert None == pool.license_exception

    def test_mirror_404_error(self):
        mirrors = dict(covers_mirror=MockS3Uploader(),books_mirror=None)
        mirror_type = ExternalIntegrationLink.COVERS
        h = DummyHTTPClient()
        h.queue_response(404)
        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)

        link = LinkData(
            rel=Hyperlink.IMAGE,
            media_type=Representation.JPEG_MEDIA_TYPE,
            href="http://example.com/",
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )

        m = Metadata(data_source=data_source)

        m.mirror_link(edition, data_source, link, link_obj, policy)

        # Since we got a 404 error, the cover image was not mirrored.
        assert 404 == link_obj.resource.representation.status_code
        assert None == link_obj.resource.representation.mirror_url
        assert [] == mirrors[mirror_type].uploaded

    def test_mirror_open_access_link_mirror_failure(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirrors = dict(covers_mirror=MockS3Uploader(fail=True))
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        content = open(self.sample_cover_path("test-book-cover.png"), "rb").read()
        link = LinkData(
            rel=Hyperlink.IMAGE,
            media_type=Representation.JPEG_MEDIA_TYPE,
            href="http://example.com/",
            content=content
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )

        h.queue_response(200, media_type=Representation.JPEG_MEDIA_TYPE)

        m.mirror_link(edition, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # The representation was fetched successfully.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None

        # But mirroring failed.
        assert representation.mirror_exception != None
        assert None == representation.mirrored_at
        assert link.media_type == representation.media_type
        assert link.href == representation.url

        # The mirror url is not set.
        assert None == representation.mirror_url

        # Book content is still there since it wasn't mirrored.
        assert representation.content != None

        # the edition's identifier-associated license pool should not be
        # suppressed just because fetch failed on getting image.
        assert False == pool.suppressed

        # the license pool only gets its license_exception column filled in
        # if fetch failed on getting an Hyperlink.OPEN_ACCESS_DOWNLOAD-type epub.
        assert None == pool.license_exception

    def test_mirror_link_bad_media_type(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirrors = dict(covers_mirror=MockS3Uploader())
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        content = open(self.sample_cover_path("test-book-cover.png"), "rb").read()

        # We thought this link was for an image file.
        link = LinkData(
            rel=Hyperlink.IMAGE,
            media_type=Representation.JPEG_MEDIA_TYPE,
            href="http://example.com/",
            content=content
        )
        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
        )

        # The remote server told us a generic media type.
        h.queue_response(200, media_type=Representation.OCTET_STREAM_MEDIA_TYPE, content=content)

        m.mirror_link(edition, data_source, link, link_obj, policy)
        representation = link_obj.resource.representation

        # The representation was fetched and mirrored successfully.
        # We assumed the original image media type was correct.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None
        assert None == representation.mirror_exception
        assert representation.mirrored_at != None
        assert Representation.JPEG_MEDIA_TYPE == representation.media_type
        assert link.href == representation.url
        assert "Gutenberg" in representation.mirror_url
        assert representation.mirror_url.endswith("%s/cover.jpg" % edition.primary_identifier.identifier)

        # We don't know the media type for this link, but it has a file extension.
        link = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/image.png",
            content=content
        )
        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
        )
        h.queue_response(200, media_type=Representation.OCTET_STREAM_MEDIA_TYPE, content=content)
        m.mirror_link(edition, data_source, link, link_obj, policy)
        representation = link_obj.resource.representation

        # The representation is still fetched and mirrored successfully.
        # We used the media type from the file extension.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None
        assert None == representation.mirror_exception
        assert representation.mirrored_at != None
        assert Representation.PNG_MEDIA_TYPE == representation.media_type
        assert link.href == representation.url
        assert "Gutenberg" in representation.mirror_url
        assert representation.mirror_url.endswith("%s/image.png" % edition.primary_identifier.identifier)

        # We don't know the media type of this link, and there's no extension.
        link = LinkData(
            rel=Hyperlink.IMAGE,
            href="http://example.com/unknown",
            content=content
        )
        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
        )
        h.queue_response(200, media_type=Representation.OCTET_STREAM_MEDIA_TYPE, content=content)
        m.mirror_link(edition, data_source, link, link_obj, policy)
        representation = link_obj.resource.representation

        # The representation is fetched, but we don't try to mirror it
        # since it doesn't have a mirrorable media type.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None
        assert None == representation.mirror_exception
        assert None == representation.mirrored_at
        assert Representation.OCTET_STREAM_MEDIA_TYPE == representation.media_type
        assert link.href == representation.url
        assert None == representation.mirror_url

    def test_non_open_access_book_not_mirrored(self):
        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirrors = dict(covers_mirror=MockS3Uploader(fail=True))
        mirror_type = ExternalIntegrationLink.COVERS
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, http_get=h.do_get)

        content = "foo"
        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href="http://example.com/",
            content=content,
            rights_uri=RightsStatus.IN_COPYRIGHT
        )

        identifier = self._identifier()
        link_obj, is_new = identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content,
        )

        # The Hyperlink object makes it look like an open-access book,
        # but the context we have from the OPDS feed says that it's
        # not.
        m.mirror_link(None, data_source, link, link_obj, policy)

        # No HTTP requests were made.
        assert [] == h.requests

        # Nothing was uploaded.
        assert [] == mirrors[mirror_type].uploaded

    def test_mirror_with_content_modifier(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirrors = dict(books_mirror=MockS3Uploader())
        mirror_type = ExternalIntegrationLink.OPEN_ACCESS_BOOKS
        def dummy_content_modifier(representation):
            representation.content = "Replaced Content"
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, content_modifier=dummy_content_modifier, http_get=h.do_get)

        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href="http://example.com/test.epub",
            content="I'm an epub",
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )

        h.queue_response(200, media_type=Representation.EPUB_MEDIA_TYPE)

        m.mirror_link(edition, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # The representation was fetched successfully.
        assert None == representation.fetch_exception
        assert representation.fetched_at != None

        # The mirror url is set.
        assert "Gutenberg" in representation.mirror_url
        assert representation.mirror_url.endswith("%s/%s.epub" % (edition.primary_identifier.identifier, edition.title))

        # Content isn't there since it was mirrored.
        assert None == representation.content

        # The representation was mirrored, with the modified content.
        assert [representation] == mirrors[mirror_type].uploaded
        assert ["Replaced Content"] == mirrors[mirror_type].content

    def test_mirror_protected_access_book(self):
        edition, pool = self._edition(with_license_pool=True)

        data_source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = Metadata(data_source=data_source)

        mirror_type = ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS
        mirrors = {mirror_type: MockS3Uploader()}

        def dummy_content_modifier(representation):
            representation.content = "Replaced Content"
        h = DummyHTTPClient()

        policy = ReplacementPolicy(mirrors=mirrors, content_modifier=dummy_content_modifier, http_get=h.do_get)

        link = LinkData(
            rel=Hyperlink.GENERIC_OPDS_ACQUISITION,
            media_type=Representation.EPUB_MEDIA_TYPE,
            href="http://example.com/test.epub",
            content="I'm an epub",
        )

        link_obj, ignore = edition.primary_identifier.add_link(
            rel=link.rel, href=link.href, data_source=data_source,
            media_type=link.media_type, content=link.content
        )

        h.queue_response(200, media_type=Representation.EPUB_MEDIA_TYPE)

        m.mirror_link(edition, data_source, link, link_obj, policy)

        representation = link_obj.resource.representation

        # The representation was fetched successfully.
        assert None == representation.fetch_exception
        assert representation.fetched_at is not None

        # The mirror url is set.
        assert "Gutenberg" in representation.mirror_url
        assert representation.mirror_url.endswith("%s/%s.epub" % (edition.primary_identifier.identifier, edition.title))

        # Content isn't there since it was mirrored.
        assert None == representation.content

        # The representation was mirrored, with the modified content.
        assert [representation] == mirrors[mirror_type].uploaded
        assert ["Replaced Content"] == mirrors[mirror_type].content

    def test_measurements(self):
        edition = self._edition()
        measurement = MeasurementData(quantity_measured=Measurement.POPULARITY,
                                      value=100)
        metadata = Metadata(measurements=[measurement],
                            data_source=edition.data_source)
        metadata.apply(edition, None)
        [m] = edition.primary_identifier.measurements
        assert Measurement.POPULARITY == m.quantity_measured
        assert 100 == m.value

    def test_coverage_record(self):
        edition, pool = self._edition(with_license_pool=True)
        data_source = edition.data_source

        # No preexisting coverage record
        coverage = CoverageRecord.lookup(edition, data_source)
        assert coverage == None

        last_update = datetime_utc(2015, 1, 1)

        m = Metadata(data_source=data_source,
                     title="New title", data_source_last_updated=last_update)
        m.apply(edition, None)

        coverage = CoverageRecord.lookup(edition, data_source)
        assert last_update == coverage.timestamp
        assert "New title" == edition.title

        older_last_update = datetime_utc(2014, 1, 1)
        m = Metadata(data_source=data_source,
                     title="Another new title",
                     data_source_last_updated=older_last_update
        )
        m.apply(edition, None)
        assert "New title" == edition.title

        coverage = CoverageRecord.lookup(edition, data_source)
        assert last_update == coverage.timestamp

        m.apply(edition, None, force=True)
        assert "Another new title" == edition.title
        coverage = CoverageRecord.lookup(edition, data_source)
        assert older_last_update == coverage.timestamp



class TestContributorData(DatabaseTest):
    def test_from_contribution(self):
        # Makes sure ContributorData.from_contribution copies all the fields over.

        # make author with that name, add author to list and pass to edition
        contributors = ["PrimaryAuthor"]
        edition, pool = self._edition(with_license_pool=True, authors=contributors)

        contribution = edition.contributions[0]
        contributor = contribution.contributor
        contributor.lc = "1234567"
        contributor.viaf = "ABC123"
        contributor.aliases = ["Primo"]
        contributor.display_name = "Test Author For The Win"
        contributor.family_name = "TestAuttie"
        contributor.wikipedia_name = "TestWikiAuth"
        contributor.biography = "He was born on Main Street."

        contributor_data = ContributorData.from_contribution(contribution)

        # make sure contributor fields are still what I expect
        assert contributor_data.lc == contributor.lc
        assert contributor_data.viaf == contributor.viaf
        assert contributor_data.aliases == contributor.aliases
        assert contributor_data.display_name == contributor.display_name
        assert contributor_data.family_name == contributor.family_name
        assert contributor_data.wikipedia_name == contributor.wikipedia_name
        assert contributor_data.biography == contributor.biography

    def test_lookup(self):
        # Test the method that uses the database to gather as much
        # self-consistent information as possible about a person.
        def m(*args, **kwargs):
            return ContributorData.lookup(self._db, *args, **kwargs)

        # We know very little about this person.
        l1, ignore = self._contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )

        # We know a lot about this person.
        pkd, ignore = self._contributor(
            sort_name="Dick, Phillip K.", display_name="Phillip K. Dick",
            viaf="27063583", lc="n79018147"
        )

        def _match(expect, actual):
            # Verify that two ContributorData objects have the
            # same data.
            #
            # If a value is None in one ContributorData, it must be None
            # in the other.
            assert isinstance(actual, ContributorData)
            assert expect.sort_name == actual.sort_name
            assert expect.display_name == actual.display_name
            assert expect.lc == actual.lc
            assert expect.viaf == actual.viaf

        # If there's no Contributor that matches the request, the method
        # returns None.
        assert None == m(sort_name="Marenghi, Garth")

        # If one and only one Contributor matches the request, the method
        # returns a ContributorData with all necessary information.
        _match(pkd, m(display_name="Phillip K. Dick"))
        _match(pkd, m(sort_name="Dick, Phillip K."))
        _match(pkd, m(viaf="27063583"))
        _match(pkd, m(lc="n79018147"))

        # If we're able to identify a Contributor from part of the
        # input, then any contradictory input is ignored in favor of
        # what we know from the database.
        _match(
            pkd,
            m(display_name="Phillip K. Dick", sort_name="Marenghi, Garth",
              viaf="1234", lc="abcd"
            )
        )

        # If we're able to identify a Contributor, but we don't know some
        # of the information, those fields are left blank.
        expect = ContributorData(
            display_name="Ann Leckie", sort_name="Leckie, Ann"
        )
        _match(expect, m(display_name="Ann Leckie"))

        # Now let's test cases where the database lookup finds
        # multiple Contributors.

        # An exact duplicate of an existing Contributor changes
        # nothing.
        duplicate, ignore = self._contributor(
            display_name="Ann Leckie",
            sort_name="Leckie, Ann",
        )
        _match(expect, m(display_name="Ann Leckie"))

        # If there's a duplicate that adds more information, multiple
        # records are consolidated, creating a synthetic
        # ContributorData that doesn't correspond to any one
        # Contributor.
        with_viaf, ignore = self._contributor(
            display_name="Ann Leckie", viaf="73520345",
        )
        # _contributor() set sort_name to a random value; remove it.
        with_viaf.sort_name = None

        expect = ContributorData(
            display_name="Ann Leckie", sort_name="Leckie, Ann",
            viaf="73520345"
        )
        _match(
            expect, m(display_name="Ann Leckie")
        )

        # Again, this works even if some of the incoming arguments
        # turn out not to be supported by the database data.
        _match(
            expect, m(display_name="Ann Leckie", sort_name="Ann Leckie",
                      viaf="abcd")
        )

        # If there's a duplicate that provides conflicting information,
        # the corresponding field is left blank -- we don't know which
        # value is correct.
        with_incorrect_viaf, ignore = self._contributor(
            display_name="Ann Leckie", viaf="abcd",
        )
        with_incorrect_viaf.sort_name=None
        expect = ContributorData(
            display_name="Ann Leckie", sort_name="Leckie, Ann",
        )
        _match(expect, m(display_name="Ann Leckie"))

        # If there's conflicting information in the database for a
        # field, but the input included a value for that field, then
        # the input value is used.
        expect.viaf = "73520345"
        _match(expect, m(display_name="Ann Leckie", viaf="73520345"))

    def test_apply(self):
        # Makes sure ContributorData.apply copies all the fields over when there's changes to be made.


        contributor_old, made_new = self._contributor(sort_name="Doe, John", viaf="viaf12345")

        kwargs = dict()
        kwargs[Contributor.BIRTH_DATE] = '2001-01-01'

        contributor_data = ContributorData(
            sort_name = "Doerr, John",
            lc = "1234567",
            viaf = "ABC123",
            aliases = ["Primo"],
            display_name = "Test Author For The Win",
            family_name = "TestAuttie",
            wikipedia_name = "TestWikiAuth",
            biography = "He was born on Main Street.",
            extra = kwargs,
        )

        contributor_new, changed = contributor_data.apply(contributor_old)

        assert changed == True
        assert contributor_new.sort_name == "Doerr, John"
        assert contributor_new.lc == "1234567"
        assert contributor_new.viaf == "ABC123"
        assert contributor_new.aliases == ["Primo"]
        assert contributor_new.display_name == "Test Author For The Win"
        assert contributor_new.family_name == "TestAuttie"
        assert contributor_new.wikipedia_name == "TestWikiAuth"
        assert contributor_new.biography == "He was born on Main Street."

        assert contributor_new.extra[Contributor.BIRTH_DATE] == "2001-01-01"
        #assert_equal(contributor_new.contributions, "Audio")

        contributor_new, changed = contributor_data.apply(contributor_new)
        assert changed == False

    def test_display_name_to_sort_name_from_existing_contributor(self):
        # If there's an existing contributor with a matching display name,
        # we'll use their sort name.
        existing_contributor, ignore = self._contributor(sort_name="Sort, Name", display_name="John Doe")
        assert "Sort, Name" == ContributorData.display_name_to_sort_name_from_existing_contributor(self._db, "John Doe")

        # Otherwise, we don't know.
        assert None == ContributorData.display_name_to_sort_name_from_existing_contributor(self._db, "Jane Doe")

    def test_find_sort_name(self):
        metadata_client = DummyMetadataClient()
        metadata_client.lookups["Metadata Client Author"] = "Author, M. C."
        existing_contributor, ignore = self._contributor(sort_name="Author, E.", display_name="Existing Author")
        contributor_data = ContributorData()

        # If there's already a sort name, keep it.
        contributor_data.sort_name = "Sort Name"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Sort Name" == contributor_data.sort_name

        contributor_data.sort_name = "Sort Name"
        contributor_data.display_name = "Existing Author"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Sort Name" == contributor_data.sort_name

        contributor_data.sort_name = "Sort Name"
        contributor_data.display_name = "Metadata Client Author"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Sort Name" == contributor_data.sort_name

        # If there's no sort name but there's already an author with the same display name,
        # use that author's sort name.
        contributor_data.sort_name = None
        contributor_data.display_name = "Existing Author"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Author, E." == contributor_data.sort_name

        # If there's no sort name and no existing author, check the metadata wrangler
        # for a sort name.
        contributor_data.sort_name = None
        contributor_data.display_name = "Metadata Client Author"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Author, M. C." == contributor_data.sort_name

        # If there's no sort name, no existing author, and nothing from the metadata
        # wrangler, guess the sort name based on the display name.
        contributor_data.sort_name = None
        contributor_data.display_name = "New Author"
        assert True == contributor_data.find_sort_name(self._db, [], metadata_client)
        assert "Author, New" == contributor_data.sort_name

    def test_find_sort_name_survives_metadata_client_exception(self):

        class Mock(ContributorData):
            # Simulate an integration error from the metadata wrangler side.
            def display_name_to_sort_name_through_canonicalizer(
                self, _db, identifiers, metadata_client
            ):
                self.called_with = (_db, identifiers, metadata_client)
                raise RemoteIntegrationException(
                    "http://url/", "Metadata wrangler failure!"
                )

        # Here's a ContributorData that's going to run into an error.
        contributor_data = Mock()
        contributor_data.display_name = "Iain M. Banks"
        identifiers = []
        metadata_client = object()
        contributor_data.find_sort_name(self._db, identifiers, metadata_client)

        # display_name_to_sort_name_through_canonicalizer was called
        # with the arguments we expect.
        assert ((self._db, identifiers, metadata_client) ==
            contributor_data.called_with)

        # Although that method raised an exception, we were able to
        # keep going and use the default display name -> sort name
        # algorithm to guess at the author name.
        assert "Banks, Iain M." == contributor_data.sort_name


class TestLinkData(DatabaseTest):
    @parameterized.expand([
        ('image', Hyperlink.IMAGE, ExternalIntegrationLink.COVERS),
        ('thumbnail', Hyperlink.THUMBNAIL_IMAGE, ExternalIntegrationLink.COVERS),
        ('open_access_book', Hyperlink.OPEN_ACCESS_DOWNLOAD, ExternalIntegrationLink.OPEN_ACCESS_BOOKS),
        ('protected_access_book', Hyperlink.GENERIC_OPDS_ACQUISITION, ExternalIntegrationLink.PROTECTED_ACCESS_BOOKS)
    ])
    def test_mirror_type_returns_correct_mirror_type_for(self, name, rel, expected_mirror_type):
        # Arrange
        link_data = LinkData(rel, href='dummy')

        # Act
        result = link_data.mirror_type()

        # Assert
        assert result == expected_mirror_type

    def test_guess_media_type(self):
        rel = Hyperlink.IMAGE

        # Sometimes we have no idea what media type is at the other
        # end of a link.
        unknown = LinkData(rel, href="http://foo/bar.unknown")
        assert None == unknown.guessed_media_type

        # Sometimes we can guess based on the file extension.
        jpeg = LinkData(rel, href="http://foo/bar.jpeg")
        assert Representation.JPEG_MEDIA_TYPE == jpeg.guessed_media_type

        # An explicitly known media type takes precedence over
        # something we guess from the file extension.
        png = LinkData(rel, href="http://foo/bar.jpeg",
                       media_type=Representation.PNG_MEDIA_TYPE)
        assert Representation.PNG_MEDIA_TYPE == png.guessed_media_type

        description = LinkData(Hyperlink.DESCRIPTION, content="Some content")
        assert None == description.guessed_media_type


class TestMetadata(DatabaseTest):
    def test_defaults(self):
        # Verify that a Metadata object doesn't make any assumptions
        # about an item's medium.
        m = Metadata(data_source=DataSource.OCLC)
        assert None == m.medium

    def test_from_edition(self):
        # Makes sure Metadata.from_edition copies all the fields over.

        edition, pool = self._edition(with_license_pool=True)
        edition.series = "Harry Otter and the Mollusk of Infamy"
        edition.series_position = "14"
        edition.primary_identifier.add_link(Hyperlink.IMAGE, "image", edition.data_source)
        metadata = Metadata.from_edition(edition)

        # make sure the metadata and the originating edition match
        for field in Metadata.BASIC_EDITION_FIELDS:
            assert getattr(edition, field) == getattr(metadata, field)

        e_contribution = edition.contributions[0]
        m_contributor_data = metadata.contributors[0]
        assert e_contribution.contributor.sort_name == m_contributor_data.sort_name
        assert e_contribution.role == m_contributor_data.roles[0]

        assert edition.data_source == metadata.data_source(self._db)
        assert edition.primary_identifier.identifier == metadata.primary_identifier.identifier

        e_link = edition.primary_identifier.links[0]
        m_link = metadata.links[0]
        assert e_link.rel == m_link.rel
        assert e_link.resource.url == m_link.href

        # The series position can also be 0.
        edition.series_position = 0
        metadata = Metadata.from_edition(edition)
        assert edition.series_position == metadata.series_position

    def test_update(self):
        # Tests that Metadata.update correctly prefers new fields to old, unless
        # new fields aren't defined.

        edition_old, pool = self._edition(with_license_pool=True)
        edition_old.publisher = "test_old_publisher"
        edition_old.subtitle = "old_subtitile"
        edition_old.series = "old_series"
        edition_old.series_position = 5
        metadata_old = Metadata.from_edition(edition_old)

        edition_new, pool = self._edition(with_license_pool=True)
        # set more fields on metadatas
        edition_new.publisher = None
        edition_new.subtitle = "new_updated_subtitile"
        edition_new.series = "new_series"
        edition_new.series_position = 0
        metadata_new = Metadata.from_edition(edition_new)

        metadata_old.update(metadata_new)

        assert metadata_old.publisher == "test_old_publisher"
        assert metadata_old.subtitle == metadata_new.subtitle
        assert metadata_old.series == edition_new.series
        assert metadata_old.series_position == edition_new.series_position

    def test_apply(self):
        edition_old, pool = self._edition(with_license_pool=True)

        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            title="The Harry Otter and the Seaweed of Ages",
            sort_title="Harry Otter and the Seaweed of Ages, The",
            subtitle="Kelp At It",
            series="The Harry Otter Sagas",
            series_position="4",
            language="eng",
            medium="Audio",
            publisher="Scholastic Inc",
            imprint="Follywood",
            published=datetime.date(1987, 5, 4),
            issued=datetime.date(1989, 4, 5)
        )

        edition_new, changed = metadata.apply(edition_old, pool.collection)

        assert changed == True
        assert edition_new.title == "The Harry Otter and the Seaweed of Ages"
        assert edition_new.sort_title == "Harry Otter and the Seaweed of Ages, The"
        assert edition_new.subtitle == "Kelp At It"
        assert edition_new.series == "The Harry Otter Sagas"
        assert edition_new.series_position == "4"
        assert edition_new.language == "eng"
        assert edition_new.medium == "Audio"
        assert edition_new.publisher == "Scholastic Inc"
        assert edition_new.imprint == "Follywood"
        assert edition_new.published == datetime.date(1987, 5, 4)
        assert edition_new.issued == datetime.date(1989, 4, 5)

        edition_new, changed = metadata.apply(edition_new, pool.collection)
        assert changed == False

        # The series position can also be 0.
        metadata.series_position = 0
        edition_new, changed = metadata.apply(edition_new, pool.collection)
        assert changed == True
        assert edition_new.series_position == 0

        # Metadata.apply() does not create a Work if no Work exists.
        assert 0 == self._db.query(Work).count()

    def test_apply_wipes_presentation_calculation_records(self):
        # We have a work.
        work = self._work(title="The Wrong Title", with_license_pool=True)

        # We learn some more information about the work's identifier.
        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=work.presentation_edition.primary_identifier,
            title="The Harry Otter and the Seaweed of Ages",
        )
        edition, ignore = metadata.edition(self._db)
        metadata.apply(edition, None)

        # The work still has the wrong title.
        assert "The Wrong Title" == work.title

        # However, the work is now slated to have its presentation
        # edition recalculated -- that will fix it.
        def assert_registered(full):
            """Verify that the WorkCoverageRecord for a full (full=True) or
            partial (full=false) presentation recalculation operation
            is in the 'registered' state, and that the
            WorkCoverageRecord for the other presentation
            recalculation operation is in the 'success' state.

            The verified WorkCoverageRecord will be reset to the 'success'
            state so that this can be called over and over without any
            extra setup.
            """
            WCR = WorkCoverageRecord
            for x in work.coverage_records:
                if x.operation == WCR.CLASSIFY_OPERATION:
                    if full:
                        assert WCR.REGISTERED == x.status
                        x.status = WCR.SUCCESS
                    else:
                        assert WCR.SUCCESS == x.status
                elif x.operation == WCR.CHOOSE_EDITION_OPERATION:
                    if full:
                        assert WCR.SUCCESS == x.status
                    else:
                        assert WCR.REGISTERED == x.status
                        x.status = WCR.SUCCESS
        assert_registered(full=False)

        # We then learn about a subject under which the work
        # is classified.
        metadata.title = None
        metadata.subjects = [SubjectData(Subject.TAG, "subject")]
        metadata.apply(edition, None)

        # The work is now slated to have its presentation completely
        # recalculated.
        record = assert_registered(full=True)

        # We then find a new description for the work.
        metadata.subjects = None
        metadata.links = [
            LinkData(rel=Hyperlink.DESCRIPTION, content="a description")
        ]
        metadata.apply(edition, None)

        # We need to do a full recalculation again.
        assert_registered(full=True)

        # We then find a new cover image for the work.
        metadata.subjects = None
        metadata.links = [
            LinkData(rel=Hyperlink.IMAGE, href="http://image/")
        ]
        metadata.apply(edition, None)

        # We need to choose a new presentation edition.
        assert_registered(full=False)


    def test_apply_identifier_equivalency(self):

        # Set up an Edition.
        edition, pool = self._edition(with_license_pool=True)

        # Create two IdentifierData objects -- one corresponding to the
        # Edition's existing Identifier, and one new one.
        primary = edition.primary_identifier
        primary_as_data = IdentifierData(
            type=primary.type, identifier=primary.identifier
        )
        other_data = IdentifierData(type="abc", identifier="def")

        # Create a Metadata object that mentions the primary
        # identifier (as an Identifier) in `primary_identifier`, but doesn't
        # mention it in `identifiers`.
        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary,
            identifiers=[other_data]
        )

        # Metadata.identifiers has two elements -- the primary and the
        # other one.
        assert 2 == len(metadata.identifiers)
        assert primary in metadata.identifiers

        # If the primary identifier is mentioned both as
        # primary_identifier and in identifiers, it shows up twice
        # in metadata.identifiers.
        metadata2 = Metadata(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary,
            identifiers=[primary_as_data, other_data]
        )
        assert 3 == len(metadata2.identifiers)
        assert primary_as_data in metadata2.identifiers
        assert primary in metadata2.identifiers
        assert other_data in metadata2.identifiers

        # Write this state of affairs to the database.
        metadata2.apply(edition, pool.collection)

        # The new identifier has been marked as equivalent to the
        # Editions' primary identifier, but the primary identifier
        # itself is untouched, even though it showed up twice in the
        # list of identifiers.
        assert 1 == len(primary.equivalencies)
        [equivalency] = primary.equivalencies
        assert equivalency.output.type == "abc"
        assert equivalency.output.identifier == "def"

    def test_apply_no_value(self):
        edition_old, pool = self._edition(with_license_pool=True)

        metadata = Metadata(
            data_source=DataSource.PRESENTATION_EDITION,
            subtitle=NO_VALUE,
            series=NO_VALUE,
            series_position=NO_NUMBER
        )

        edition_new, changed = metadata.apply(edition_old, pool.collection)

        assert changed == True
        assert edition_new.title == edition_old.title
        assert edition_new.sort_title == edition_old.sort_title
        assert edition_new.subtitle == None
        assert edition_new.series == None
        assert edition_new.series_position == None
        assert edition_new.language == edition_old.language
        assert edition_new.medium == edition_old.medium
        assert edition_new.publisher == edition_old.publisher
        assert edition_new.imprint == edition_old.imprint
        assert edition_new.published == edition_old.published
        assert edition_new.issued == edition_old.issued

    def test_apply_creates_coverage_records(self):
        edition, pool = self._edition(with_license_pool=True)

        metadata = Metadata(
            data_source=DataSource.OVERDRIVE,
            title=self._str
        )

        edition, changed = metadata.apply(edition, pool.collection)

        # One success was recorded.
        records = self._db.query(
            CoverageRecord
        ).filter(
            CoverageRecord.identifier_id==edition.primary_identifier.id
        ).filter(
            CoverageRecord.operation==None
        )
        assert 1 == records.count()
        assert CoverageRecord.SUCCESS == records.all()[0].status

        # No metadata upload failure was recorded, because this metadata
        # came from Overdrive.
        records = self._db.query(
            CoverageRecord
        ).filter(
            CoverageRecord.identifier_id==edition.primary_identifier.id
        ).filter(
            CoverageRecord.operation==CoverageRecord.METADATA_UPLOAD_OPERATION
        )
        assert 0 == records.count()

        # Apply metadata from a different source.
        metadata = Metadata(
            data_source=DataSource.GUTENBERG,
            title=self._str
        )

        edition, changed = metadata.apply(edition, pool.collection)

        # Another success record was created.
        records = self._db.query(
            CoverageRecord
        ).filter(
            CoverageRecord.identifier_id==edition.primary_identifier.id
        ).filter(
            CoverageRecord.operation==None
        )
        assert 2 == records.count()
        for record in records.all():
            assert CoverageRecord.SUCCESS == record.status

        # But now there's also a metadata upload failure.
        records = self._db.query(
            CoverageRecord
        ).filter(
            CoverageRecord.identifier_id==edition.primary_identifier.id
        ).filter(
            CoverageRecord.operation==CoverageRecord.METADATA_UPLOAD_OPERATION
        )
        assert 1 == records.count()
        assert CoverageRecord.TRANSIENT_FAILURE == records.all()[0].status



    def test_update_contributions(self):
        edition = self._edition()

        # A test edition is created with a test contributor. This
        # particular contributor is about to be destroyed and replaced by
        # new data.
        [old_contributor] = edition.contributors

        contributor = ContributorData(
            display_name="Robert Jordan",
            sort_name="Jordan, Robert",
            wikipedia_name="Robert_Jordan",
            viaf="79096089",
            lc="123",
            roles=[Contributor.PRIMARY_AUTHOR_ROLE]
        )

        metadata = Metadata(DataSource.OVERDRIVE, contributors=[contributor])
        metadata.update_contributions(self._db, edition, replace=True)

        # The old contributor has been removed and replaced with the new
        # one.
        [contributor] = edition.contributors
        assert contributor != old_contributor

        # And the new one has all the information provided by
        # the Metadata object.
        assert "Jordan, Robert" == contributor.sort_name
        assert "Robert Jordan" == contributor.display_name
        assert "79096089" == contributor.viaf
        assert "123" == contributor.lc
        assert "Robert_Jordan" == contributor.wikipedia_name

    def test_filter_recommendations(self):
        metadata = Metadata(DataSource.OVERDRIVE)
        known_identifier = self._identifier()
        unknown_identifier = IdentifierData(Identifier.ISBN, "hey there")

        # Unknown identifiers are filtered out of the recommendations.
        metadata.recommendations += [known_identifier, unknown_identifier]
        metadata.filter_recommendations(self._db)
        assert [known_identifier] == metadata.recommendations

        # It works with IdentifierData as well.
        known_identifier_data = IdentifierData(
            known_identifier.type, known_identifier.identifier
        )
        metadata.recommendations = [known_identifier_data, unknown_identifier]
        metadata.filter_recommendations(self._db)
        [result] = metadata.recommendations
        # The IdentifierData has been replaced by a bonafide Identifier.
        assert True == isinstance(result, Identifier)
        # The genuwine article.
        assert known_identifier == result


    def test_metadata_can_be_deepcopied(self):
        # Check that we didn't put something in the metadata that
        # will prevent it from being copied. (e.g., self.log)

        subject = SubjectData(Subject.TAG, "subject")
        contributor = ContributorData()
        identifier = IdentifierData(Identifier.GUTENBERG_ID, "1")
        link = LinkData(Hyperlink.OPEN_ACCESS_DOWNLOAD, "example.epub")
        measurement = MeasurementData(Measurement.RATING, 5)
        circulation = CirculationData(data_source=DataSource.GUTENBERG,
            primary_identifier=identifier,
            licenses_owned=0,
            licenses_available=0,
            licenses_reserved=0,
            patrons_in_hold_queue=0)
        primary_as_data = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )
        other_data = IdentifierData(type="abc", identifier="def")

        m = Metadata(
            DataSource.GUTENBERG,
            subjects=[subject],
            contributors=[contributor],
            primary_identifier=identifier,
            links=[link],
            measurements=[measurement],
            circulation=circulation,

            title="Hello Title",
            subtitle="Subtle Hello",
            sort_title="Sorting Howdy",
            language="US English",
            medium=Edition.BOOK_MEDIUM,
            series="1",
            series_position=1,
            publisher="Hello World Publishing House",
            imprint="Follywood",
            issued=utc_now(),
            published=utc_now(),
            identifiers=[primary_as_data, other_data],
            data_source_last_updated=utc_now(),
        )

        m_copy = deepcopy(m)

        # If deepcopy didn't throw an exception we're ok.
        assert m_copy is not None


    def test_links_filtered(self):
        # test that filter links to only metadata-relevant ones
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
        metadata = Metadata(
            data_source=DataSource.GUTENBERG,
            primary_identifier=identifier,
            links=links,
        )

        filtered_links = sorted(metadata.links, key=lambda x:x.rel)

        assert [link2, link5, link4, link3] == filtered_links


class TestCirculationData(DatabaseTest):

    def test_apply_propagates_analytics(self):
        # Verify that an Analytics object is always passed into
        # license_pool() and update_availability(), even if none is
        # provided in the ReplacementPolicy.
        #
        # NOTE: this test was written to verify a bug fix; it's not a
        # comprehensive test of CirculationData.apply().
        source = DataSource.lookup(self._db, DataSource.GUTENBERG)
        identifier = self._identifier()
        collection = self._default_collection

        class MockLicensePool(object):
            # A LicensePool-like object that tracks how its
            # update_availability() method was called.
            delivery_mechanisms = []
            licenses = []
            work = None
            def calculate_work(self):
                return None, False
            def update_availability(self, **kwargs):
                self.update_availability_called_with = kwargs

        pool = MockLicensePool()
        class MockCirculationData(CirculationData):
            # A CirculationData-like object that always says
            # update_availability ought to be called on a
            # specific MockLicensePool.
            def license_pool(self, _db, collection, analytics):
                self.license_pool_called_with = (_db, collection, analytics)
                return pool, False

            def _availability_needs_update(self, *args):
                # Force update_availability to be called.
                return True

        # First try with no particular ReplacementPolicy.
        data = MockCirculationData(source, identifier)
        data.apply(self._db, collection)

        # A generic Analytics object was created and passed in to
        # MockCirculationData.license_pool().
        analytics1 = data.license_pool_called_with[-1]
        assert isinstance(analytics1, Analytics)

        # Then, the same Analytics object was passed into the
        # update_availability() method of the MockLicensePool returned
        # by license_pool()
        analytics2 = pool.update_availability_called_with['analytics']
        assert analytics1 == analytics2

        # Now try with a ReplacementPolicy that mentions a specific
        # analytics object.
        analytics = object()
        policy = ReplacementPolicy(analytics=analytics)
        data.apply(self._db, collection, replace=policy)

        # That object was used instead of a generic Analytics object in
        # both cases.
        assert analytics == data.license_pool_called_with[-1]
        assert analytics == pool.update_availability_called_with['analytics']


class TestTimestampData(DatabaseTest):

    def test_constructor(self):

        # By default, all fields are set to None
        d = TimestampData()
        for i in (d.service, d.service_type, d.collection_id,
                  d.start, d.finish, d.achievements, d.counter,
                  d.exception):
            assert i == None

        # Some, but not all, of the fields can be set to real values.
        d = TimestampData(start="a", finish="b", achievements="c",
                          counter="d", exception="e")
        assert "a" == d.start
        assert "b" == d.finish
        assert "c" == d.achievements
        assert "d" == d.counter
        assert "e" == d.exception

    def test_is_failure(self):
        # A TimestampData represents failure if its exception is set to
        # any value other than None or CLEAR_VALUE.
        d = TimestampData()
        assert False == d.is_failure

        d.exception = "oops"
        assert True == d.is_failure

        d.exception = None
        assert False == d.is_failure

        d.exception = d.CLEAR_VALUE
        assert False == d.is_failure

    def test_is_complete(self):
        # A TimestampData is complete if it represents a failure
        # (see above) or if its .finish is set to any value other
        # than None or CLEAR_VALUE

        d = TimestampData()
        assert False == d.is_complete

        d.finish = "done!"
        assert True == d.is_complete

        d.finish = None
        assert False == d.is_complete

        d.finish = d.CLEAR_VALUE
        assert False == d.is_complete

        d.exception = "oops"
        assert True == d.is_complete

    def test_finalize_minimal(self):
        # Calling finalize() with only the minimal arguments sets the
        # timestamp values to sensible defaults and leaves everything
        # else alone.

        # This TimestampData starts out with everything set to None.
        d = TimestampData()
        d.finalize("service", "service_type", self._default_collection)

        # finalize() requires values for these arguments, and sets them.
        assert "service" == d.service
        assert "service_type" == d.service_type
        assert self._default_collection.id == d.collection_id

        # The timestamp values are set to sensible defaults.
        assert d.start == d.finish
        assert (utc_now() - d.start).total_seconds() < 2

        # Other fields are still at None.
        for i in d.achievements, d.counter, d.exception:
            assert i == None

    def test_finalize_full(self):
        # You can call finalize() with a complete set of arguments.
        d = TimestampData()
        d.finalize(
            "service", "service_type", self._default_collection,
            start="start", finish="finish", counter="counter",
            exception="exception"
        )
        assert "start" == d.start
        assert "finish" == d.finish
        assert "counter" == d.counter
        assert "exception" == d.exception

        # If the TimestampData fields are already set to values other
        # than CLEAR_VALUE, the required fields will be overwritten but
        # the optional fields will be left alone.
        new_collection = self._collection()
        d.finalize(
            "service2", "service_type2", new_collection,
            start="start2", finish="finish2", counter="counter2",
            exception="exception2"
        )
        # These have changed.
        assert "service2" == d.service
        assert "service_type2" == d.service_type
        assert new_collection.id == d.collection_id

        # These have not.
        assert "start" == d.start
        assert "finish" == d.finish
        assert "counter" == d.counter
        assert "exception" == d.exception

    def test_collection(self):
        d = TimestampData()
        d.finalize("service", "service_type", self._default_collection)
        assert self._default_collection == d.collection(self._db)

    def test_apply(self):

        # You can't apply a TimestampData that hasn't been finalized.
        d = TimestampData()
        with pytest.raises(ValueError) as excinfo:
            d.apply(self._db)
        assert "Not enough information to write TimestampData to the database." in str(excinfo.value)

        # Set the basic timestamp information. Optional fields will stay
        # at None.
        collection = self._default_collection
        d.finalize("service", Timestamp.SCRIPT_TYPE, collection)
        d.apply(self._db)
        now = utc_now()

        timestamp = Timestamp.lookup(
            self._db, "service", Timestamp.SCRIPT_TYPE, collection
        )
        assert (now-timestamp.start).total_seconds() < 2
        assert timestamp.start == timestamp.finish

        # Now set the optional fields as well.
        d.counter = 100
        d.achievements = "yay"
        d.exception = "oops"
        d.apply(self._db)

        assert 100 == timestamp.counter
        assert "yay" == timestamp.achievements
        assert "oops" == timestamp.exception

        # We can also use apply() to clear out the values for all
        # fields other than the ones that uniquely identify the
        # Timestamp.
        clear = TimestampData.CLEAR_VALUE
        d.start = clear
        d.finish = clear
        d.counter = clear
        d.achievements = clear
        d.exception = clear
        d.apply(self._db)

        assert None == timestamp.start
        assert None == timestamp.finish
        assert None == timestamp.counter
        assert None == timestamp.achievements
        assert None == timestamp.exception


class TestAssociateWithIdentifiersBasedOnPermanentWorkID(DatabaseTest):

    def test_success(self):
        pwid = 'pwid1'

        # Here's a print book.
        book = self._edition()
        book.medium = Edition.BOOK_MEDIUM
        book.permanent_work_id = pwid

        # Here's an audio book with the same PWID.
        audio = self._edition()
        audio.medium = Edition.AUDIO_MEDIUM
        audio.permanent_work_id=pwid

        # Here's an Metadata object for a second print book with the
        # same PWID.
        identifier = self._identifier()
        identifierdata = IdentifierData(
            type=identifier.type, identifier=identifier.identifier
        )
        metadata = Metadata(
            DataSource.GUTENBERG,
            primary_identifier=identifierdata, medium=Edition.BOOK_MEDIUM
        )
        metadata.permanent_work_id=pwid

        # Call the method we're testing.
        metadata.associate_with_identifiers_based_on_permanent_work_id(
            self._db
        )

        # The identifier of the second print book has been associated
        # with the identifier of the first print book, but not
        # with the identifier of the audiobook
        equivalent_identifiers = [x.output for x in identifier.equivalencies]
        assert [book.primary_identifier] == equivalent_identifiers


class TestMARCExtractor(DatabaseTest):

    def setup_method(self):
        super(TestMARCExtractor, self).setup_method()
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "marc")

    def sample_data(self, filename):
        with open(os.path.join(self.resource_path, filename), "rb") as fh:
            return fh.read()

    def test_parse_year(self):
        m = MARCExtractor.parse_year
        nineteen_hundred = strptime_utc("1900", "%Y")
        assert nineteen_hundred == m("1900")
        assert nineteen_hundred == m("1900.")
        assert None == m("not a year")

    def test_parser(self):
        """Parse a MARC file into Metadata objects."""

        file = self.sample_data("ils_plympton_01.mrc")
        metadata_records = MARCExtractor.parse(file, "Plympton")

        assert 36 == len(metadata_records)

        record = metadata_records[1]
        assert "Strange Case of Dr Jekyll and Mr Hyde" == record.title
        assert "Stevenson, Robert Louis" == record.contributors[0].sort_name
        assert "Recovering the Classics" in record.publisher
        assert "9781682280041" == record.primary_identifier.identifier
        assert Identifier.ISBN == record.primary_identifier.type
        subjects = record.subjects
        assert 2 == len(subjects)
        for s in subjects:
            assert Classifier.FAST == s.type
        assert "Canon" in subjects[0].identifier
        assert Edition.BOOK_MEDIUM == record.medium
        assert 2015 == record.issued.year
        assert 'eng' == record.language

        assert 1 == len(record.links)
        assert "Utterson and Enfield are worried about their friend" in record.links[0].content

    def test_name_cleanup(self):
        """Test basic name cleanup techniques."""
        m = MARCExtractor.name_cleanup
        assert "Dante Alighieri" == m("Dante Alighieri,   1265-1321, author.")
        assert "Stevenson, Robert Louis" == m("Stevenson, Robert Louis.")
        assert "Wells, H.G." == m("Wells,     H.G.")
