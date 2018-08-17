# encoding: utf-8

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

import datetime
from dateutil.relativedelta import relativedelta
import json
import os

from classifier import Classifier
from coverage import CoverageFailure

from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    Identifier,
    Hyperlink,
    LicensePool,
    Representation,
    Subject,
    Work,
)

from oneclick import (
    OneClickAPI,
    MockOneClickAPI,
    OneClickBibliographicCoverageProvider,
    OneClickRepresentationExtractor,
    OneClickDeltaMonitor,
    OneClickImportMonitor,
)

from util.http import (
    BadResponseException,
    RemoteIntegrationException,
    HTTP,
)

from . import DatabaseTest
from scripts import RunCollectionCoverageProviderScript
from testing import MockRequestsResponse


class OneClickTest(DatabaseTest):

    def setup(self):
        super(OneClickTest, self).setup()
        base_path = os.path.split(__file__)[0]
        self.collection = MockOneClickAPI.mock_collection(self._db)
        self.api = MockOneClickAPI(
            self._db, self.collection, base_path=base_path
        )


class TestOneClickAPI(OneClickTest):

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = OneClickAPI.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)


    def test_availability_exception(self):
        self.api.queue_response(500)
        assert_raises_regexp(
            BadResponseException, "Bad response from availability_search",
            self.api.get_all_available_through_search
        )


    def test_search(self):
        datastr, datadict = self.api.get_data("response_search_one_item_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response = self.api.search(mediatype='ebook', author="Alexander Mccall Smith", title="Tea Time for the Traditionally Built")
        response_dictionary = response.json()
        eq_(1, response_dictionary['pageCount'])
        eq_(u'Tea Time for the Traditionally Built', response_dictionary['items'][0]['item']['title'])


    def test_get_all_available_through_search(self):
        datastr, datadict = self.api.get_data("response_search_five_items_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_dictionary = self.api.get_all_available_through_search()
        eq_(1, response_dictionary['pageCount'])
        eq_(5, response_dictionary['resultSetCount'])
        eq_(5, len(response_dictionary['items']))
        returned_titles = [iteminterest['item']['title'] for iteminterest in response_dictionary['items']]
        assert (u'Unusual Uses for Olive Oil' in returned_titles)


    def test_get_all_catalog(self):
        datastr, datadict = self.api.get_data("response_catalog_all_sample.json")
        self.api.queue_response(status_code=200, content=datastr)

        catalog = self.api.get_all_catalog()
        eq_(8, len(catalog))
        eq_("Challenger Deep", catalog[7]['title'])


    def test_get_delta(self):
        datastr, datadict = self.api.get_data("response_catalog_delta.json")
        self.api.queue_response(status_code=200, content=datastr)

        assert_raises_regexp(
            ValueError, 'from_date 2000-01-01 00:00:00 must be real, in the past, and less than 6 months ago.',
            self.api.get_delta, from_date="2000-01-01", to_date="2000-02-01"
        )

        today = datetime.datetime.now()
        three_months = relativedelta(months=3)
        assert_raises_regexp(
            ValueError, "from_date .* - to_date .* asks for too-wide date range.",
            self.api.get_delta, from_date=(today - three_months), to_date=today
        )

        delta = self.api.get_delta()
        eq_(1931, delta[0]["libraryId"])
        eq_("Wethersfield Public Library", delta[0]["libraryName"])
        eq_("2016-10-17", delta[0]["beginDate"])
        eq_("2016-10-18", delta[0]["endDate"])
        eq_(0, delta[0]["eBookAddedCount"])
        eq_(0, delta[0]["eBookRemovedCount"])
        eq_(1, delta[0]["eAudioAddedCount"])
        eq_(1, delta[0]["eAudioRemovedCount"])
        eq_(1, delta[0]["titleAddedCount"])
        eq_(1, delta[0]["titleRemovedCount"])
        eq_(1, len(delta[0]["addedTitles"]))
        eq_(1, len(delta[0]["removedTitles"]))


    def test_get_ebook_availability_info(self):
        datastr, datadict = self.api.get_data("response_availability_ebook_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_list = self.api.get_ebook_availability_info()
        eq_(u'9781420128567', response_list[0]['isbn'])
        eq_(False, response_list[0]['availability'])


    def test_get_metadata_by_isbn(self):
        datastr, datadict = self.api.get_data("response_isbn_notfound_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        response_dictionary = self.api.get_metadata_by_isbn('97BADISBNFAKE')
        eq_(None, response_dictionary)


        self.api.queue_response(status_code=404, content="{}")
        assert_raises_regexp(
            BadResponseException,
            "Bad response from .*",
            self.api.get_metadata_by_isbn, identifier='97BADISBNFAKE'
        )

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        self.api.queue_response(status_code=200, content=datastr)
        response_dictionary = self.api.get_metadata_by_isbn('9780307378101')
        eq_(u'9780307378101', response_dictionary['isbn'])
        eq_(u'Anchor', response_dictionary['publisher'])



class TestOneClickRepresentationExtractor(OneClickTest):

    def test_book_info_with_metadata(self):
        # Tests that can convert a oneclick json block into a Metadata object.

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        metadata = OneClickRepresentationExtractor.isbn_info_to_metadata(datadict)

        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.sort_title)
        eq_(None, metadata.subtitle)
        eq_(Edition.BOOK_MEDIUM, metadata.medium)
        eq_("No. 1 Ladies Detective Agency", metadata.series)
        eq_(10, metadata.series_position)
        eq_("eng", metadata.language)
        eq_("Anchor", metadata.publisher)
        eq_(None, metadata.imprint)
        eq_(2013, metadata.published.year)
        eq_(12, metadata.published.month)
        eq_(27, metadata.published.day)

        [author1, author2, narrator] = metadata.contributors
        eq_(u"Mccall Smith, Alexander", author1.sort_name)
        eq_(u"Alexander Mccall Smith", author1.display_name)
        eq_([Contributor.AUTHOR_ROLE], author1.roles)
        eq_(u"Wilder, Thornton", author2.sort_name)
        eq_(u"Thornton Wilder", author2.display_name)
        eq_([Contributor.AUTHOR_ROLE], author2.roles)

        eq_(u"Guskin, Laura Flanagan", narrator.sort_name)
        eq_(u"Laura Flanagan Guskin", narrator.display_name)
        eq_([Contributor.NARRATOR_ROLE], narrator.roles)

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)

        eq_([(None, u"FICTION / Humorous / General", Subject.BISAC, 100),

            (u'adult', None, Classifier.RBDIGITAL_AUDIENCE, 500),

            (u'humorous-fiction', None, Subject.RBDIGITAL, 200),
            (u'mystery', None, Subject.RBDIGITAL, 200),
            (u'womens-fiction', None, Subject.RBDIGITAL, 200)
         ],
            [(x.identifier, x.name, x.type, x.weight) for x in subjects]
        )

        # Related IDs.
        eq_((Identifier.ONECLICK_ID, '9780307378101'),
            (metadata.primary_identifier.type, metadata.primary_identifier.identifier))

        ids = [(x.type, x.identifier) for x in metadata.identifiers]

        # We made exactly one OneClick and one ISBN-type identifiers.
        eq_(
            [(Identifier.ISBN, "9780307378101"), (Identifier.ONECLICK_ID, "9780307378101")],
            sorted(ids)
        )

        # Available formats.
        [epub] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.EPUB_MEDIA_TYPE, epub.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, epub.drm_scheme)

        # Links to various resources.
        shortd, image = sorted(
            metadata.links, key=lambda x:x.rel
        )

        eq_(Hyperlink.SHORT_DESCRIPTION, shortd.rel)
        assert shortd.content.startswith("THE NO. 1 LADIES' DETECTIVE AGENCY")

        eq_(Hyperlink.IMAGE, image.rel)
        eq_('http://images.oneclickdigital.com/EB00148140/EB00148140_image_128x192.jpg', image.href)

        thumbnail = image.thumbnail

        eq_(Hyperlink.THUMBNAIL_IMAGE, thumbnail.rel)
        eq_('http://images.oneclickdigital.com/EB00148140/EB00148140_image_95x140.jpg', thumbnail.href)

        # Note: For now, no measurements associated with the book.

        # Request only the bibliographic information.
        metadata = OneClickRepresentationExtractor.isbn_info_to_metadata(datadict, include_bibliographic=True, include_formats=False)
        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.circulation)

        # Request only the format information.
        metadata = OneClickRepresentationExtractor.isbn_info_to_metadata(datadict, include_bibliographic=False, include_formats=True)
        eq_(None, metadata.title)
        [epub] = sorted(metadata.circulation.formats, key=lambda x: x.content_type)
        eq_(Representation.EPUB_MEDIA_TYPE, epub.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, epub.drm_scheme)


    def test_book_info_metadata_no_series(self):
        """'Default Blank' is not a series -- it's a string representing
        the absence of a series.
        """

        datastr, datadict = self.api.get_data("response_isbn_found_no_series.json")
        metadata = OneClickRepresentationExtractor.isbn_info_to_metadata(datadict)

        eq_("Tea Time for the Traditionally Built", metadata.title)
        eq_(None, metadata.series)
        eq_(None, metadata.series_position)


class TestOneClickBibliographicCoverageProvider(OneClickTest):
    """Test the code that looks up bibliographic information from OneClick."""

    def setup(self):
        super(TestOneClickBibliographicCoverageProvider, self).setup()

        self.provider = OneClickBibliographicCoverageProvider(
            self.collection, api_class=MockOneClickAPI,
            api_class_kwargs=dict(base_path=os.path.split(__file__)[0])
        )
        self.api = self.provider.api

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            OneClickBibliographicCoverageProvider, self._db,
            api_class=MockOneClickAPI
        )
        [provider] = script.providers
        assert isinstance(provider,
                          OneClickBibliographicCoverageProvider)
        assert isinstance(provider.api, MockOneClickAPI)
        eq_(self.collection, provider.collection)

    def test_invalid_or_unrecognized_guid(self):
        # A bad or malformed ISBN can't get coverage.

        identifier = self._identifier()
        identifier.identifier = 'ISBNbadbad'

        datastr, datadict = self.api.get_data("response_isbn_notfound_1.json")
        self.api.queue_response(status_code=200, content=datastr)

        failure = self.provider.process_item(identifier)
        assert isinstance(failure, CoverageFailure)
        eq_(True, failure.transient)
        assert failure.exception.startswith('Cannot find OneClick metadata')


    def test_process_item_creates_presentation_ready_work(self):
        # Test the normal workflow where we ask OneClick for data,
        # OneClick provides it, and we create a presentation-ready work.

        datastr, datadict = self.api.get_data("response_isbn_found_1.json")
        self.api.queue_response(200, content=datastr)

        # Here's the book mentioned in response_isbn_found_1.
        identifier = self._identifier(identifier_type=Identifier.ONECLICK_ID)
        identifier.identifier = '9780307378101'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the OneClickBibliographicCoverageProvider
        result = self.provider.process_item(identifier)
        eq_(identifier, result)

        # A LicensePool was created. But we do NOT know how many copies of this
        # book are available, only what formats it's available in.
        [pool] = identifier.licensed_through
        eq_(0, pool.licenses_owned)
        [lpdm] = pool.delivery_mechanisms
        eq_('application/epub+zip (application/vnd.adobe.adept+xml)', lpdm.delivery_mechanism.name)

        # A Work was created and made presentation ready.
        eq_('Tea Time for the Traditionally Built', pool.work.title)
        eq_(True, pool.work.presentation_ready)


class TestOneClickSyncMonitor(DatabaseTest):

    # TODO: The only thing this should test is that the monitors can
    # be instantiated using the constructor arguments used by
    # RunCollectionMonitorScript, and that calling run_once() results
    # in a call to the appropriate OneClickAPI method.
    #
    # However, there's no other code that tests populate_all_catalog()
    # or populate_delta(), so we can't just remove the code; we need to
    # refactor the tests.

    def setup(self):
        super(TestOneClickSyncMonitor, self).setup()
        self.base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(self.base_path, "files", "oneclick")
        self.collection = MockOneClickAPI.mock_collection(self._db)

    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)

    def test_import(self):

        # Create a OneClickImportMonitor, which will take the current
        # state of a OneClick collection and mirror the whole thing to
        # a local database.
        monitor = OneClickImportMonitor(
            self._db, self.collection, api_class=MockOneClickAPI,
            api_class_kwargs=dict(base_path=self.base_path)
        )
        datastr, datadict = self.get_data("response_catalog_all_sample.json")
        monitor.api.queue_response(status_code=200, content=datastr)
        monitor.run()

        # verify that we created Works, Editions, LicensePools
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals",
            "In-Flight Russian", "Road, The", "Private Patient, The",
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The",
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        # make sure we created some Editions
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9780062231727", create_if_not_exists=False)
        assert(edition is not None)
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9781615730186", create_if_not_exists=False)
        assert(edition is not None)

        # make sure we created some LicensePools
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9780062231727", collection=self.collection
        )
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        eq_(False, made_new)
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9781615730186", collection=self.collection
        )
        eq_(False, made_new)
        eq_(1, pool.licenses_owned)
        eq_(1, pool.licenses_available)

        # make sure there are 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))

        #
        # Now we're going to run the delta monitor to change things
        # around a bit.
        #

        # set license numbers on test pool to match what's in the
        # delta document.
        pool, made_new = LicensePool.for_foreign_id(
            self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID,
            "9781615730186", collection=self.collection
        )
        eq_(False, made_new)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 2
        pool.patrons_in_hold_queue = 1

        # now update that library with a sample delta
        delta_monitor = OneClickDeltaMonitor(
            self._db, self.collection, api_class=MockOneClickAPI,
            api_class_kwargs=dict(base_path=self.base_path)
        )
        datastr, datadict = self.get_data("response_catalog_delta.json")
        delta_monitor.api.queue_response(status_code=200, content=datastr)
        delta_monitor.run()

        # "Tricks" did not get deleted, but did get its pools set to "nope".
        # "Emperor Mage: The Immortals" got new metadata.
        works = self._db.query(Work).all()
        work_titles = [work.title for work in works]
        expected_titles = ["Tricks", "Emperor Mage: The Immortals",
            "In-Flight Russian", "Road, The", "Private Patient, The",
            "Year of Magical Thinking, The", "Junkyard Bot: Robots Rule, Book 1, The",
            "Challenger Deep"]
        eq_(set(expected_titles), set(work_titles))

        eq_("Tricks", pool.presentation_edition.title)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)
        assert (datetime.datetime.utcnow() - pool.last_checked) < datetime.timedelta(seconds=20)

        # make sure we updated fields
        edition = Edition.for_foreign_id(self._db, DataSource.ONECLICK, Identifier.ONECLICK_ID, "9781934180723", create_if_not_exists=False)
        eq_("Recorded Books, Inc.", edition.publisher)

        # make sure there are still 8 LicensePools
        pools = self._db.query(LicensePool).all()
        eq_(8, len(pools))

        # Running the monitor again does nothing. Since no more responses
        # are queued, doing any work at this point would crash the test.
        eq_((0,0), monitor.invoke())
