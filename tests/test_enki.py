from nose.tools import (
    assert_raises,
    assert_raises_regexp,
    set_trace,
    eq_,
    assert_not_equal,
    raises,
)
import datetime
import os
import pkgutil
import json
from core.model import (
    CirculationEvent,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Measurement,
    Representation,
    Resource,
    Subject,
    Timestamp,
    Work,
)
from . import DatabaseTest
from api.authenticator import BasicAuthenticationProvider
from api.circulation import LoanInfo
from api.circulation_exceptions import *
from api.config import CannotLoadConfiguration
from api.enki import (
    BibliographicParser,
    EnkiAPI,
    EnkiCollectionReaper,
    EnkiImport,
    MockEnkiAPI,
)
from core.metadata_layer import (
    CirculationData,
    Metadata,
)
from core.scripts import RunCollectionCoverageProviderScript
from core.util.http import (
    BadResponseException,
    RequestTimedOut,
)
from core.testing import MockRequestsResponse

class BaseEnkiTest(DatabaseTest):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "enki")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()

    def setup(self):
        super(BaseEnkiTest, self).setup()
        self.api = MockEnkiAPI(self._db)
        self.collection = self.api.collection


class TestEnkiAPI(BaseEnkiTest):

    def test_constructor(self):

        bad_collection = self._collection(
            protocol=ExternalIntegration.OVERDRIVE
        )
        assert_raises_regexp(
            ValueError,
            "Collection protocol is Overdrive, but passed into EnkiAPI!",
            EnkiAPI, self._db, bad_collection
        )

        # Without an external_account_id, an EnkiAPI cannot be instantiated.
        bad_collection.protocol = ExternalIntegration.ENKI
        assert_raises_regexp(
            CannotLoadConfiguration,
            "Enki configuration is incomplete.",
            EnkiAPI,
            self._db,
            bad_collection
        )

        bad_collection.external_account_id = "1"
        EnkiAPI(self._db, bad_collection)

    def test_external_integration(self):
        integration = self.api.external_integration(self._db)
        eq_(ExternalIntegration.ENKI, integration.protocol)

    def test_collection(self):
        eq_(self.collection, self.api.collection)

    def test__run_self_tests(self):
        # Mock every method that will be called by the self-test.
        class Mock(MockEnkiAPI):
            def recent_activity(self, minutes):
                self.recent_activity_called_with = minutes
                yield 1
                yield 2

            def updated_titles(self, minutes):
                self.updated_titles_called_with = minutes
                yield 1
                yield 2
                yield 3

            patron_activity_called_with = []
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with.append((patron, pin))
                yield 1

        api = Mock(self._db)

        # Now let's make sure two Libraries have access to the
        # Collection used in the API -- one library with a default
        # patron and one without.
        no_default_patron = self._library()
        api.collection.libraries.append(no_default_patron)

        with_default_patron = self._default_library
        integration = self._external_integration(
            "api.simple_authentication",
            ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=[with_default_patron]
        )
        p = BasicAuthenticationProvider
        integration.setting(p.TEST_IDENTIFIER).value = "username1"
        integration.setting(p.TEST_PASSWORD).value = "password1"

        # Now that everything is set up, run the self-test.
        no_patron_activity, default_patron_activity, circulation_changes, collection_changes = sorted(
            api._run_self_tests(self._db),
            key=lambda x: x.name
        )

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        eq_(60, api.recent_activity_called_with)
        eq_(True, circulation_changes.success)
        eq_("2 circulation events in the last hour", circulation_changes.result)

        eq_(1440, api.updated_titles_called_with)
        eq_(True, collection_changes.success)
        eq_("3 titles added/updated in the last day", collection_changes.result)

        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            no_patron_activity.name
        )
        eq_(False, no_patron_activity.success)
        eq_("Library has no test patron configured.",
            no_patron_activity.exception.message)

        eq_(
            "Checking patron activity, using test patron for library %s" % with_default_patron.name,
            default_patron_activity.name
        )
        eq_(True, default_patron_activity.success)
        eq_("Total loans and holds: 1", default_patron_activity.result)


    def test_request_retried_once(self):
        """A request that times out is retried."""
        class TimesOutOnce(EnkiAPI):
            timeout = True
            called_with = []
            def _request(self, *args, **kwargs):
                self.called_with.append((args, kwargs))
                if self.timeout:
                    self.timeout = False
                    raise RequestTimedOut("url", "timeout")
                else:
                    return MockRequestsResponse(200, content="content")

        api = TimesOutOnce(self._db, self.collection)
        response = api.request("url")

        # Two identical requests were made.
        r1, r2 = api.called_with
        eq_(r1, r2)

        # In the end, we got our content.
        eq_(200, response.status_code)
        eq_("content", response.content)

    def test_request_retried_only_once(self):
        """A request that times out twice is not retried."""
        class TimesOut(EnkiAPI):
            calls = 0
            def _request(self, *args, **kwargs):
                self.calls += 1
                raise RequestTimedOut("url", "timeout")

        api = TimesOut(self._db, self.collection)
        assert_raises(RequestTimedOut, api.request, "url")

        # Only two requests were made.
        eq_(2, api.calls)

    def test__minutes_since(self):
        """Test the _minutes_since helper method."""
        an_hour_ago = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=60
        )
        eq_(60, EnkiAPI._minutes_since(an_hour_ago))

    def test_recent_activity(self):
        one_minute_ago = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=1
        )
        data = self.get_data("get_recent_activity.json")
        self.api.queue_response(200, content=data)
        activity = list(self.api.recent_activity(one_minute_ago))
        eq_(43, len(activity))
        for i in activity:
            assert isinstance(i, CirculationData)
        [method, url, headers, data, params, kwargs] = self.api.requests.pop()
        eq_("get", method)
        eq_("https://enkilibrary.org/API/ItemAPI", url)
        eq_("getRecentActivity", params['method'])
        eq_("c", params['lib'])
        eq_(1, params['minutes'])

    def test_updated_titles(self):
        one_minute_ago = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=1
        )
        data = self.get_data("get_update_titles.json")
        self.api.queue_response(200, content=data)
        activity = list(self.api.updated_titles(since=one_minute_ago))

        eq_(6, len(activity))
        for i in activity:
            assert isinstance(i, Metadata)

        eq_("Nervous System", activity[0].title)
        eq_(1, activity[0].circulation.licenses_owned)

        [method, url, headers, data, params, kwargs] = self.api.requests.pop()
        eq_("get", method)
        eq_("https://enkilibrary.org/API/ListAPI", url)
        eq_("getUpdateTitles", params['method'])
        eq_("c", params['lib'])
        eq_(1, params['minutes'])

    def test_get_item(self):
        data = self.get_data("get_item_french_title.json")
        self.api.queue_response(200, content=data)
        metadata = self.api.get_item("an id")

        # We get a Metadata with associated CirculationData.
        assert isinstance(metadata, Metadata)
        eq_("Le But est le Seul Choix", metadata.title)
        eq_(1, metadata.circulation.licenses_owned)

        [method, url, headers, data, params, kwargs] = self.api.requests.pop()
        eq_("get", method)
        eq_("https://enkilibrary.org/API/ItemAPI", url)
        eq_("an id", params["recordid"])
        eq_("getItem", params["method"])
        eq_("c", params['lib'])

        # We asked for a large cover image in case this Metadata is
        # to be used to form a local picture of the book's metadata.
        eq_("large", params['size'])

    def test_get_item_not_found(self):
        self.api.queue_response(200, content="<html>No such book</html>")
        metadata = self.api.get_item("an id")
        eq_(None, metadata)

    def test_get_all_titles(self):
        # get_all_titles and get_update_titles return data in the same
        # format, so we can use one to mock the other.
        data = self.get_data("get_update_titles.json")
        self.api.queue_response(200, content=data)

        results = list(self.api.get_all_titles())
        eq_(6, len(results))
        metadata = results[0]
        assert isinstance(metadata, Metadata)
        eq_("Nervous System", metadata.title)
        eq_(1, metadata.circulation.licenses_owned)

        [method, url, headers, data, params, kwargs] = self.api.requests.pop()
        eq_("get", method)
        eq_("https://enkilibrary.org/API/ListAPI", url)
        eq_("c", params['lib'])
        eq_("getAllTitles", params['method'])
        eq_("secontent", params['id'])

    def test__epoch_to_struct(self):
        """Test the _epoch_to_struct helper method."""
        eq_(datetime.datetime(1970, 1, 1), EnkiAPI._epoch_to_struct("0"))

    def test_checkout_open_access(self):
        """Test that checkout info for non-ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_direct.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        loan = self.api.parse_patron_loans(result['result']['checkedOutItems'][0])
        eq_(loan.data_source_name, DataSource.ENKI)
        eq_(loan.identifier_type, Identifier.ENKI_ID)
        eq_(loan.identifier, "2")
        eq_(loan.start_date, datetime.datetime(2017, 8, 23, 19, 31, 58, 0))
        eq_(loan.end_date, datetime.datetime(2017, 9, 13, 19, 31, 58, 0))

    def test_checkout_acs(self):
        """Test that checkout info for ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_acs.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        loan = self.api.parse_patron_loans(result['result']['checkedOutItems'][0])
        eq_(loan.data_source_name, DataSource.ENKI)
        eq_(loan.identifier_type, Identifier.ENKI_ID)
        eq_(loan.identifier, "3334")
        eq_(loan.start_date, datetime.datetime(2017, 8, 23, 19, 42, 35, 0))
        eq_(loan.end_date, datetime.datetime(2017, 9, 13, 19, 42, 35, 0))

    @raises(AuthorizationFailedException)
    def test_checkout_bad_authorization(self):
        """Test that the correct exception is thrown upon an unsuccessful login."""
        data = self.get_data("login_unsuccessful.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)

        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True
        )
        pool.identifier.identifier = 'notanid'

        patron = self._patron(external_identifier='notabarcode')

        loan = self.api.checkout(patron,'notapin',pool,None)

    @raises(NoAvailableCopies)
    def test_checkout_not_available(self):
        """Test that the correct exception is thrown upon an unsuccessful login."""
        data = self.get_data("no_copies.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)

        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True
        )
        pool.identifier.identifier = 'econtentRecord1'
        patron = self._patron(external_identifier='12345678901234')

        loan = self.api.checkout(patron,'1234',pool,None)

    def test_fulfillment_open_access(self):
        """Test that fulfillment info for non-ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_direct.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        fulfill_data = self.api.parse_fulfill_result(result['result'])
        eq_(fulfill_data[0], """http://cccl.enkilibrary.org/API/UserAPI?method=downloadEContentFile&username=21901000008080&password=deng&lib=1&recordId=2""")
        eq_(fulfill_data[1], 'epub')

    def test_fulfillment_acs(self):
        """Test that fulfillment info for ACS Enki books is parsed correctly."""
        data = self.get_data("checked_out_acs.json")
        self.api.queue_response(200, content=data)
        result = json.loads(data)
        fulfill_data = self.api.parse_fulfill_result(result['result'])
        eq_(fulfill_data[0], """http://afs.enkilibrary.org/fulfillment/URLLink.acsm?action=enterloan&ordersource=Califa&orderid=ACS4-9243146841581187248119581&resid=urn%3Auuid%3Ad5f54da9-8177-43de-a53d-ef521bc113b4&gbauthdate=Wed%2C+23+Aug+2017+19%3A42%3A35+%2B0000&dateval=1503517355&rights=%24lat%231505331755%24&gblver=4&auth=8604f0fc3f014365ea8d3c4198c721ed7ed2c16d""")
        eq_(fulfill_data[1], 'epub')

    def test_patron_activity(self):
        data = self.get_data("patron_response.json")
        self.api.queue_response(200, content=data)
        patron = self._patron()
        [loan] = self.api.patron_activity(patron, 'pin')
        assert isinstance(loan, LoanInfo)

        eq_(Identifier.ENKI_ID, loan.identifier_type)
        eq_(DataSource.ENKI, loan.data_source_name)
        eq_("231", loan.identifier)
        eq_(self.collection, loan.collection(self._db))
        eq_(datetime.datetime(2017, 8, 15, 14, 56, 51), loan.start_date)
        eq_(datetime.datetime(2017, 9, 5, 14, 56, 51), loan.end_date)

    def test_patron_activity_failure(self):
        patron = self._patron()
        self.api.queue_response(404, "No such patron")
        collect = lambda: list(self.api.patron_activity(patron, 'pin'))
        assert_raises(PatronNotFoundOnRemote, collect)

        msg = dict(result=dict(message="Login unsuccessful."))
        self.api.queue_response(200, content=json.dumps(msg))
        assert_raises(AuthorizationFailedException, collect)

        msg = dict(result=dict(message="Some other error."))
        self.api.queue_response(200, content=json.dumps(msg))
        assert_raises(CirculationException, collect)

class TestBibliographicParser(BaseEnkiTest):

    def test_process_all(self):
        class Mock(BibliographicParser):
            inputs = []
            def extract_bibliographic(self, element):
                self.inputs.append(element)

        parser = Mock()
        def consume(*args):
            """Consume a generator's output."""
            list(parser.process_all(*args))

        # First try various inputs that run successfully but don't
        # extract any data.
        consume("{}")
        eq_([], parser.inputs)

        consume(dict(result=dict()))
        eq_([], parser.inputs)

        consume(dict(result=dict(titles=[])))
        eq_([], parser.inputs)

        # Now try a list of books that is split up and each book
        # processed separately.
        data = self.get_data("get_update_titles.json")
        consume(data)
        eq_(6, len(parser.inputs))

    def test_extract_bibliographic(self):
        """Test the ability to turn an individual book data blob
        into a Metadata.
        """
        data = json.loads(self.get_data("get_item_french_title.json"))
        parser = BibliographicParser()
        m = parser.extract_bibliographic(data['result'])
        assert isinstance(m, Metadata)

        eq_(u'Le But est le Seul Choix', m.title)
        eq_(u'fre', m.language)
        eq_(u'Law of Time Press', m.publisher)

        # Two identifiers, Enki and ISBN, with Enki being primary.
        enki, isbn = sorted(m.identifiers, key=lambda x: x.type)
        eq_(Identifier.ENKI_ID, enki.type)
        eq_("21135", enki.identifier)
        eq_(enki, m.primary_identifier)

        eq_(Identifier.ISBN, isbn.type)
        eq_("9780988432727", isbn.identifier)

        # One contributor
        [contributor] = m.contributors
        eq_("Hoffmeister, David", contributor.sort_name)
        eq_([Contributor.AUTHOR_ROLE], contributor.roles)

        # Two links -- full-sized image and description.
        image, description = sorted(m.links, key=lambda x: x.rel)
        eq_(Hyperlink.IMAGE, image.rel)
        eq_("https://enkilibrary.org/bookcover.php?id=21135&isbn=9780988432727&category=EMedia&size=large", image.href)

        eq_(Hyperlink.DESCRIPTION, description.rel)
        eq_("text/html", description.media_type)
        assert description.content.startswith("David Hoffmeister r&eacute;")

        # The full-sized image has a thumbnail.
        eq_(Hyperlink.THUMBNAIL_IMAGE, image.thumbnail.rel)
        eq_("http://thumbnail/", image.thumbnail.href)

        # Four subjects.
        subjects = sorted(m.subjects, key=lambda x: x.identifier)

        # All subjects are classified as tags, rather than BISAC, due
        # to inconsistencies in the data presentation.
        for i in subjects:
            eq_(i.type, Subject.TAG)
            eq_(None, i.name)
        eq_([u'BODY MIND SPIRIT Spirituality General',
             u'BODY, MIND & SPIRIT / Spirituality / General.',
             u'Spirituality',
             u'Spirituality.'],
            [x.identifier for x in subjects]
        )

        # We also have information about the current availability.
        circulation = m.circulation
        assert isinstance(circulation, CirculationData)
        eq_(1, circulation.licenses_owned)
        eq_(1, circulation.licenses_available)
        eq_(0, circulation.licenses_reserved)
        eq_(0, circulation.patrons_in_hold_queue)

        # The book is available as an ACS-encrypted EPUB.
        [format] = circulation.formats
        eq_(Representation.EPUB_MEDIA_TYPE, format.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, format.drm_scheme)

    def test_extract_bibliographic_pdf(self):
        """Test the ability to distingush between PDF and EPUB results"""
        data = json.loads(self.get_data("pdf_document_entry.json"))
        parser = BibliographicParser()
        m = parser.extract_bibliographic(data['result'])
        assert isinstance(m, Metadata)

        # The book is available as a non-ACS PDF.
        circulation = m.circulation
        assert isinstance(circulation, CirculationData)
        [format] = circulation.formats
        eq_(Representation.PDF_MEDIA_TYPE, format.content_type)
        eq_(DeliveryMechanism.NO_DRM, format.drm_scheme)

class TestEnkiImport(BaseEnkiTest):

    def test_import_instantiation(self):
        """Test that EnkiImport can be instantiated"""
        importer = EnkiImport(self._db, self.collection, api_class=self.api)
        eq_(self.api, importer.api)
        eq_(self.collection, importer.collection)

    def test_run_once(self):
        dummy_value = object()
        class Mock(EnkiImport):
            incremental_import_called_with = dummy_value
            def full_import(self):
                self.full_import_called = True

            def incremental_import(self, since):
                self.incremental_import_called_with = since

        importer = Mock(self._db, self.collection, api_class=self.api)

        # If run_once() is called with no start time, as happens the first time
        # the importer runs, it calls full_import().
        importer.run_once(None, None)
        eq_(True, importer.full_import_called)

        # It doesn't call incremental_import().
        eq_(dummy_value, importer.incremental_import_called_with)

        # If run_once() is called with a start time, a time five
        # minutes previous is passed into incremental_import()
        importer.full_import_called = False
        timestamp = datetime.datetime.utcnow()
        five_minutes_earlier = timestamp - importer.FIVE_MINUTES
        importer.run_once(timestamp, None)

        passed_in = importer.incremental_import_called_with
        assert abs((passed_in-five_minutes_earlier).total_seconds()) < 2

        # full_import was not called.
        eq_(False, importer.full_import_called)

    def test_full_import(self):
        """full_import calls get_all_titles over and over again until
        it returns nothing, and processes every book it receives.
        """
        class MockAPI(object):
            def __init__(self, pages):
                """Act like an Enki API with predefined pages of results."""
                self.pages = pages
                self.get_all_titles_called_with = []

            def get_all_titles(self, strt, qty):
                self.get_all_titles_called_with.append((strt, qty))
                if self.pages:
                    return self.pages.pop(0)
                return []

        class Mock(EnkiImport):
            processed = []
            def process_book(self, data):
                self.processed.append(data)

        # Simulate an Enki site with two pages of results.
        pages = [[1,2], [3]]
        api = MockAPI(pages)

        # Do the 'import'.
        importer = Mock(self._db, self.collection, api_class=api)
        importer.full_import()

        # get_all_titles was called three times, once for the first two
        # pages and a third time to verify that there are no more results.
        eq_([(0, 10), (10, 10), (20, 10)],
            api.get_all_titles_called_with)

        # Every item on every 'page' of results was processed.
        eq_([1,2,3], importer.processed)

    def test_incremental_import(self):
        """incremental_import calls process_book() on the output of
        EnkiAPI.updated_titles(), and then calls update_circulation().
        """
        class MockAPI(object):
            def updated_titles(self, since):
                self.updated_titles_called_with = since
                yield 1
                yield 2

        class Mock(EnkiImport):
            processed = []
            def process_book(self, data):
                self.processed.append(data)

            def update_circulation(self, since):
                self.update_circulation_called_with = since

        api = MockAPI()
        importer = Mock(self._db, self.collection, api_class=api)
        since = object()
        importer.incremental_import(since)

        # The 'since' value was passed into both methods.
        eq_(since, api.updated_titles_called_with)
        eq_(since, importer.update_circulation_called_with)

        # The two items yielded by updated_titles() were run
        # through process_book().
        eq_([1,2], importer.processed)

    def test_update_circulation(self):

        # Here's information about a book we didn't know about before.
        circ_data = {"result":{"records":1,"recentactivity":[{"historyid":"3738","id":"34278","recordId":"econtentRecord34278","time":"2018-06-26 10:08:23","action":"Checked Out","isbn":"9781618856050","availability":{"accessType":"acs","totalCopies":"1","availableCopies":0,"onHold":0}}]}}

        # Because the book is unknown, update_circulation will do a follow-up
        # call to api.get_item to get bibliographic information.
        bib_data = {"result":{"id":"34278","recordId":"econtentRecord34278","isbn":"9781618856050","title":"A book","availability":{"accessType":"acs","totalCopies":"1","availableCopies":0,"onHold":0}}}

        api = MockEnkiAPI(self._db)
        api.queue_response(200, content=json.dumps(circ_data))
        api.queue_response(200, content=json.dumps(bib_data))

        from core.mock_analytics_provider import MockAnalyticsProvider
        analytics = MockAnalyticsProvider()
        monitor = EnkiImport(self._db, self.collection, api_class=api,
                             analytics=analytics)
        since = datetime.datetime.utcnow()
        monitor.update_circulation(since)

        # Two requests were made -- one to getRecentActivity
        # and one to getItem.
        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        eq_('get', method)
        eq_('https://enkilibrary.org/API/ItemAPI', url)
        eq_('getRecentActivity', params['method'])
        eq_(0, params['minutes'])

        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        eq_('get', method)
        eq_('https://enkilibrary.org/API/ItemAPI', url)
        eq_('getItem', params['method'])
        eq_('34278', params['recordid'])

        # We ended up with one Work, one LicensePool, and one Edition.
        work = self._db.query(Work).one()
        licensepool = self._db.query(LicensePool).one()
        edition = self._db.query(Edition).one()
        eq_([licensepool], work.license_pools)
        eq_(edition, licensepool.presentation_edition)

        identifier = licensepool.identifier
        eq_(Identifier.ENKI_ID, identifier.type)
        eq_(u"34278", identifier.identifier)

        # The LicensePool and Edition take their data from the mock API
        # requests.
        eq_("A book", work.title)
        eq_(1, licensepool.licenses_owned)
        eq_(0, licensepool.licenses_available)

        # An analytics event was sent out for the newly discovered book.
        eq_(1, analytics.count)

        # Now let's see what update_circulation does when the work
        # already exists.
        circ_data['result']['recentactivity'][0]['availability']['totalCopies'] = 10
        api.queue_response(200, content=json.dumps(circ_data))
        # We're not queuing up more bib data, but that's no problem --
        # EnkiImport won't ask for it.

        # Pump the monitor again.
        monitor.update_circulation(since)

        # We made a single request, to getRecentActivity.
        [method, url, headers, data, params, kwargs] = api.requests.pop(0)
        eq_('getRecentActivity', params['method'])

        # The LicensePool was updated, but no new objects were created.
        eq_(10, licensepool.licenses_owned)
        for c in (LicensePool, Edition, Work):
            eq_(1, self._db.query(c).count())

    def test_process_book(self):
        """This functionality is tested as part of
        test_update_circulation.
        """
        pass


class TestEnkiCollectionReaper(BaseEnkiTest):

    def test_book_that_doesnt_need_reaping_is_left_alone(self):
        # We're happy with this book.
        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            identifier_id="21135",
            data_source_name=DataSource.ENKI,
            with_license_pool=True,
            collection=self.collection
        )
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.patrons_in_hold_queue = 5

        # Enki still considers this book to be in the library's
        # collection.
        data = self.get_data("get_item_french_title.json")
        self.api.queue_response(200, content=data)

        reaper = EnkiCollectionReaper(
            self._db, self.collection, api_class=self.api
        )

        # Run the identifier through the reaper.
        reaper.process_item(pool.identifier)

        # The book was left alone.
        eq_(10, pool.licenses_owned)
        eq_(9, pool.licenses_available)
        eq_(5, pool.patrons_in_hold_queue)

    def test_reaped_book_has_zero_licenses(self):
        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.ENKI_ID,
            data_source_name=DataSource.ENKI,
            with_license_pool=True,
            collection=self.collection
        )

        # This is a specific record ID that should never exist
        nonexistent_id = "econtentRecord0"

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        pool.identifier.identifier = nonexistent_id
        eq_(None, pool.last_checked)

        # Enki will claim it doesn't know about this book
        # by sending an HTML error instead of JSON data.
        data = "<html></html>"
        self.api.queue_response(200, content=data)

        reaper = EnkiCollectionReaper(
            self._db, self.collection, api_class=self.api
        )

        # Run the identifier through the reaper.
        reaper.process_item(pool.identifier)

        # The item's circulation information has been zeroed out.
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
