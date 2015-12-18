from StringIO import StringIO
from nose.tools import (
    eq_,
    set_trace,
)
import pkgutil
import csv

from metadata_layer import (
    CSVFormatError,
    CSVMetadataImporter,
    MeasurementData,
    FormatData,
    LinkData,
    Metadata,
)

import os
from model import (
    DataSource,
    Identifier,
    Measurement,
    DeliveryMechanism,
    Hyperlink,
    Representation,
)

from . import (
    DatabaseTest,
)

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

        eq_(u"Horrorst\xf6r", m1.title)
        eq_("Grady Hendrix", m1.contributors[0].display_name)
        eq_("Martin Jensen", m2.contributors[0].display_name)

        # Let's check out the identifiers we found.

        # The first book has an Overdrive ID
        [overdrive] = m1.identifiers
        eq_(Identifier.OVERDRIVE_ID, overdrive.type)
        eq_('504BA8F6-FF4E-4B57-896E-F1A50CFFCA0C', overdrive.identifier)
        eq_(1, overdrive.weight)

        # The second book has no ID at all.
        eq_([], m2.identifiers)

        # The third book has both a 3M ID and an Overdrive ID.
        overdrive, threem = sorted(m3.identifiers, key=lambda x: x.identifier)

        eq_(Identifier.OVERDRIVE_ID, overdrive.type)
        eq_('eae60d41-e0b8-4f9d-90b5-cbc43d433c2f', overdrive.identifier)
        eq_(1, overdrive.weight)

        eq_(Identifier.THREEM_ID, threem.type)
        eq_('eswhyz9', threem.identifier)
        eq_(1, threem.weight)

        # Now let's check out subjects.
        eq_(
            [
                ('schema:typicalAgeRange', u'Adult', 100),
                ('tag', u'Character Driven', 100),
                ('tag', u'Historical', 100), 
                ('tag', u'Nail-Biters', 100),
                ('tag', u'Setting Driven', 100)
            ],
            [(x.type, x.identifier, x.weight) 
             for x in sorted(m2.subjects, key=lambda x: x.identifier)]
        )


    def test_links(self):
        edition = self._edition()
        l1 = LinkData(rel=Hyperlink.IMAGE, href="http://example.com/")
        l2 = LinkData(rel=Hyperlink.DESCRIPTION, content="foo")
        metadata = Metadata(links=[l1, l2], 
                            data_source=edition.data_source)
        metadata.apply(edition)
        [image, description] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )
        eq_(Hyperlink.IMAGE, image.rel)
        eq_("http://example.com/", image.resource.url)

        eq_(Hyperlink.DESCRIPTION, description.rel)
        eq_("foo", description.resource.representation.content)

    def test_image_and_thumbnail(self):
        edition = self._edition()
        l2 = LinkData(
            rel=Hyperlink.THUMBNAIL_IMAGE, href="http://thumbnail.com/"
        )
        l1 = LinkData(
            rel=Hyperlink.IMAGE, href="http://example.com/", thumbnail=l2
        )
        metadata = Metadata(links=[l1, l2], 
                            data_source=edition.data_source)
        metadata.apply(edition)
        [image, thumbnail] = sorted(
            edition.primary_identifier.links, key=lambda x:x.rel
        )
        eq_(Hyperlink.IMAGE, image.rel)
        eq_([thumbnail.resource.representation],
            image.resource.representation.thumbnails
        )

    def test_measurements(self):
        edition = self._edition()
        measurement = MeasurementData(quantity_measured=Measurement.POPULARITY,
                                      value=100)
        metadata = Metadata(measurements=[measurement],
                            data_source=edition.data_source)
        metadata.apply(edition)
        [m] = edition.primary_identifier.measurements
        eq_(Measurement.POPULARITY, m.quantity_measured)
        eq_(100, m.value)

    def test_explicit_formatdata(self):
        # Creating an edition with an open-access download will
        # automatically create a delivery mechanism.
        edition, pool = self._edition(with_open_access_download=True)

        # Let's also add a DRM format.
        drm_format = FormatData(
            content_type=Representation.PDF_MEDIA_TYPE,
            drm_scheme=DeliveryMechanism.ADOBE_DRM,
        )

        metadata = Metadata(formats=[drm_format],
                            data_source=edition.data_source)
        metadata.apply(edition)

        [epub, pdf] = sorted(pool.delivery_mechanisms, 
                             key=lambda x: x.delivery_mechanism.content_type)
        eq_(epub.resource, edition.best_open_access_link)

        eq_(Representation.PDF_MEDIA_TYPE, pdf.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, pdf.delivery_mechanism.drm_scheme)

        # If we tell Metadata to replace the list of formats, we only
        # have the one format we manually created.
        metadata.apply(edition, replace_formats=True)
        [pdf] = pool.delivery_mechanisms
        eq_(Representation.PDF_MEDIA_TYPE, pdf.delivery_mechanism.content_type)

    def test_implicit_format_for_open_access_link(self):
        edition, pool = self._edition(with_license_pool=True)

        # This is the delivery mechanism created by default when you
        # create a book with _edition().
        [epub] = pool.delivery_mechanisms
        eq_(Representation.EPUB_MEDIA_TYPE, epub.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, epub.delivery_mechanism.drm_scheme)


        link = LinkData(
            rel=Hyperlink.OPEN_ACCESS_DOWNLOAD,
            media_type=Representation.PDF_MEDIA_TYPE,
            href=self._url
        )
        metadata = Metadata(
            data_source=DataSource.GUTENBERG, 
            links=[link]
        )
        metadata.apply(edition, replace_formats=True)

        # We destroyed the default delivery format and added a new,
        # open access delivery format.
        [pdf] = pool.delivery_mechanisms
        eq_(Representation.PDF_MEDIA_TYPE, pdf.delivery_mechanism.content_type)
        eq_(DeliveryMechanism.NO_DRM, pdf.delivery_mechanism.drm_scheme)

        metadata = Metadata(
            data_source=DataSource.GUTENBERG, 
            links=[]
        )
        metadata.apply(edition, replace_links=True, replace_formats=True)

        # Now we have no formats at all.
        eq_([], pool.delivery_mechanisms)


