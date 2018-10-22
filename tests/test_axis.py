import datetime
import json
import os
from lxml import etree
from StringIO import StringIO
from nose.tools import (
    eq_,
    assert_raises,
    assert_raises_regexp,
    set_trace,
)

from core.analytics import Analytics
from core.coverage import CoverageFailure

from core.model import (
    ConfigurationSetting,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Representation,
    Subject,
    create,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
)

from core.scripts import RunCollectionCoverageProviderScript
from core.testing import MockRequestsResponse

from core.util.http import (
    RemoteIntegrationException,
    HTTP,
)

from api.authenticator import BasicAuthenticationProvider

from api.axis import (
    AvailabilityResponseParser,
    Axis360API,
    Axis360BibliographicCoverageProvider,
    Axis360CirculationMonitor,
    AxisCollectionReaper,
    BibliographicParser,
    CheckoutResponseParser,
    FulfillmentInfoResponseParser,
    HoldReleaseResponseParser,
    HoldResponseParser,
    MockAxis360API,
    ResponseParser,
)

from . import (
    DatabaseTest,
    sample_data
)

from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)

from api.circulation_exceptions import *

from api.config import (
    Configuration,
    temp_config,
)

from api.web_publication_manifest import FindawayManifest


class Axis360Test(DatabaseTest):

    def setup(self):
        super(Axis360Test,self).setup()
        self.collection = MockAxis360API.mock_collection(self._db)
        self.api = MockAxis360API(self._db, self.collection)

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')


class TestAxis360API(Axis360Test):

    def test_external_integration(self):
        eq_(
            self.collection.external_integration,
            self.api.external_integration(object())
        )

    def test__run_self_tests(self):
        """Verify that BibliothecaAPI._run_self_tests() calls the right
        methods.
        """
        class Mock(MockAxis360API):
            "Mock every method used by Axis360API._run_self_tests."

            # First we will refresh the bearer token.
            def refresh_bearer_token(self):
                return "the new token"

            # Then we will count the number of events in the past
            # give minutes.
            def recent_activity(self, since):
                self.recent_activity_called_with = since
                return [(1,"a"),(2, "b"), (3, "c")]

            # Then we will count the loans and holds for the default
            # patron.
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with = (patron, pin)
                return ["loan", "hold"]

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = self._library()
        self.collection.libraries.append(no_default_patron)

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
        api = Mock(self._db, self.collection)
        now = datetime.datetime.utcnow()
        [no_patron_credential, recent_circulation_events, patron_activity,
         refresh_bearer_token] = sorted(
            api._run_self_tests(self._db), key=lambda x: x.name
        )
        eq_("Refreshing bearer token", refresh_bearer_token.name)
        eq_(True, refresh_bearer_token.success)
        eq_("the new token", refresh_bearer_token.result)

        eq_(
            "Acquiring test patron credentials for library %s" % no_default_patron.name,
            no_patron_credential.name
        )
        eq_(False, no_patron_credential.success)
        eq_("Library has no test patron configured.",
            no_patron_credential.exception.message)

        eq_("Asking for circulation events for the last five minutes",
            recent_circulation_events.name)
        eq_(True, recent_circulation_events.success)
        eq_("Found 3 event(s)", recent_circulation_events.result)
        since = api.recent_activity_called_with
        five_minutes_ago = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
        assert (five_minutes_ago-since).total_seconds() < 2

        eq_("Checking activity for test patron for library %s" % with_default_patron.name,
            patron_activity.name)
        eq_(True, patron_activity.success)
        eq_("Found 2 loans/holds", patron_activity.result)
        patron, pin = api.patron_activity_called_with
        eq_("username1", patron.authorization_identifier)
        eq_("password1", pin)

    def test__run_self_tests_short_circuit(self):
        """If we can't refresh the bearer token, the rest of the
        self-tests aren't even run.
        """
        class Mock(MockAxis360API):
            def refresh_bearer_token(self):
                raise Exception("no way")

        # Now that everything is set up, run the self-test. Only one
        # test will be run.
        api = Mock(self._db, self.collection)
        [failure] = api._run_self_tests(self._db)
        eq_("Refreshing bearer token", failure.name)
        eq_(False, failure.success)
        eq_("no way", failure.exception.message)

    def test_create_identifier_strings(self):
        identifier = self._identifier()
        values = Axis360API.create_identifier_strings(["foo", identifier])
        eq_(["foo", identifier.identifier], values)

    def test_availability_exception(self):

        self.api.queue_response(500)
        assert_raises_regexp(
            RemoteIntegrationException, "Bad response from http://axis.test/availability/v2: Got status code 500 from external server, cannot continue.",
            self.api.availability
        )

    def test_refresh_bearer_token_after_401(self):
        """If we get a 401, we will fetch a new bearer token and try the
        request again.
        """
        self.api.queue_response(401)
        self.api.queue_response(
            200, content=json.dumps(dict(access_token="foo"))
        )
        self.api.queue_response(200, content="The data")
        response = self.api.request("http://url/")
        eq_("The data", response.content)

    def test_refresh_bearer_token_error(self):
        """Raise an exception if we don't get a 200 status code when
        refreshing the bearer token.
        """
        api = MockAxis360API(self._db, self.collection, with_token=False)
        api.queue_response(412)
        assert_raises_regexp(
            RemoteIntegrationException, "Bad response from http://axis.test/accesstoken: Got status code 412 from external server, but can only continue on: 200.",
            api.refresh_bearer_token
        )

    def test_exception_after_401_with_fresh_token(self):
        """If we get a 401 immediately after refreshing the token, we will
        raise an exception.
        """
        self.api.queue_response(401)
        self.api.queue_response(
            200, content=json.dumps(dict(access_token="foo"))
        )
        self.api.queue_response(401)

        self.api.queue_response(301)

        assert_raises_regexp(
            RemoteIntegrationException,
            ".*Got status code 401 from external server, cannot continue.",
            self.api.request, "http://url/"
        )

        # The fourth request never got made.
        eq_([301], [x.status_code for x in self.api.responses])

    def test_update_availability(self):
        """Test the Axis 360 implementation of the update_availability method
        defined by the CirculationAPI interface.
        """

        # Create a LicensePool that needs updating.
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True,
            collection=self.collection
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        eq_(None, pool.last_checked)

        # Prepare availability information.
        data = self.sample_data("availability_with_loans.xml")

        # Modify the data so that it appears to be talking about the
        # book we just created.
        new_identifier = pool.identifier.identifier.encode("ascii")
        data = data.replace("0012533119", new_identifier)

        self.api.queue_response(200, content=data)

        self.api.update_availability(pool)

        # The availability information has been udpated, as has the
        # date the availability information was last checked.
        eq_(2, pool.licenses_owned)
        eq_(1, pool.licenses_available)
        eq_(0, pool.patrons_in_hold_queue)
        assert pool.last_checked is not None

    def test_place_hold(self):
        edition, pool = self._edition(
            identifier_type=Identifier.AXIS_360_ID,
            data_source_name=DataSource.AXIS_360,
            with_license_pool=True
        )
        data = self.sample_data("place_hold_success.xml")
        self.api.queue_response(200, content=data)
        patron = self._patron()
        ConfigurationSetting.for_library(
            Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
            self._default_library).value = "notifications@example.com"
        response = self.api.place_hold(patron, 'pin', pool, None)
        eq_(1, response.hold_position)
        eq_(response.identifier_type, pool.identifier.type)
        eq_(response.identifier, pool.identifier.identifier)
        [request] = self.api.requests
        params = request[-1]['params']
        eq_('notifications@example.com', params['email'])

    def test_update_licensepools_for_identifiers(self):

        class Mock(MockAxis360API):
            """Simulates an Axis 360 API that knows about some
            books but not others.
            """
            updated = []
            reaped = []

            def _fetch_remote_availability(self, identifiers):
                for i, identifier in enumerate(identifiers):
                    # The first identifer in the list is still
                    # available.
                    identifier_data = IdentifierData(
                        type=identifier.type,
                        identifier=identifier.identifier
                    )
                    metadata = Metadata(
                        data_source=DataSource.AXIS_360,
                        primary_identifier=identifier_data
                    )
                    availability = CirculationData(
                        data_source=DataSource.AXIS_360,
                        primary_identifier=identifier_data,
                        licenses_owned=7,
                        licenses_available=6
                    )
                    yield metadata, availability

                    # The rest have been 'forgotten' by Axis 360.
                    break

            def _reap(self, identifier):
                self.reaped.append(identifier)

        api = Mock(self._db, self.collection)
        still_in_collection = self._identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        no_longer_in_collection = self._identifier(
            identifier_type=Identifier.AXIS_360_ID
        )
        api.update_licensepools_for_identifiers(
            [still_in_collection, no_longer_in_collection]
        )

        # The LicensePool for the first identifier was updated.
        [lp] = still_in_collection.licensed_through
        eq_(7, lp.licenses_owned)
        eq_(6, lp.licenses_available)

        # The second was reaped.
        eq_([no_longer_in_collection], api.reaped)

    def test_fetch_remote_availability(self):
        """Test the _fetch_remote_availability method, as
        used by update_licensepools_for_identifiers.
        """
        id1 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        id2 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        data = self.sample_data("availability_with_loans.xml")
        # Modify the sample data so that it appears to be talking
        # about one of the books we're going to request.
        data = data.replace("0012533119", id1.identifier.encode("ascii"))
        self.api.queue_response(200, {}, data)
        results = [x for x in self.api._fetch_remote_availability([id1, id2])]

        # We asked for information on two identifiers.
        [request] = self.api.requests
        kwargs = request[-1]
        eq_({'titleIds': u'2001,2002'}, kwargs['params'])

        # We got information on only one.
        [(metadata, circulation)] = results
        eq_((id1, False), metadata.primary_identifier.load(self._db))
        eq_(u'El caso de la gracia : Un periodista explora las evidencias de unas vidas transformadas', metadata.title)
        eq_(2, circulation.licenses_owned)

    def test_reap(self):
        """Test the _reap method, as used by
        update_licensepools_for_identifiers.
        """
        id1 = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        eq_([], id1.licensed_through)

        # If there is no LicensePool to reap, nothing happens.
        self.api._reap(id1)
        eq_([], id1.licensed_through)

        # If there is a LicensePool but it has no owned licenses,
        # it's already been reaped, so nothing happens.
        edition, pool, = self._edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type, identifier_id=id1.identifier,
            with_license_pool=True, collection=self.collection
        )

        # This LicensePool has licenses, but it's not in a different
        # collection from the collection associated with this
        # Axis360API object, so it's not affected.
        collection2 = self._collection()
        edition2, pool2, = self._edition(
            data_source_name=DataSource.AXIS_360,
            identifier_type=id1.type, identifier_id=id1.identifier,
            with_license_pool=True, collection=collection2
        )

        pool.licenses_owned = 0
        pool2.licenses_owned = 10
        self._db.commit()
        updated = pool.last_checked
        updated2 = pool2.last_checked
        self.api._reap(id1)

        eq_(updated, pool.last_checked)
        eq_(0, pool.licenses_owned)
        eq_(updated2, pool2.last_checked)
        eq_(10, pool2.licenses_owned)

        # If the LicensePool did have licenses, then reaping it
        # reflects the fact that the licenses are no longer owned.
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7
        self.api._reap(id1)
        eq_(0, pool.licenses_owned)
        eq_(0, pool.licenses_available)
        eq_(0, pool.licenses_reserved)
        eq_(0, pool.patrons_in_hold_queue)

    def test_get_fulfillment_info(self):
        # Test the get_fulfillment_info method, which makes an API request.

        api = MockAxis360API(self._db, self.collection)
        api.queue_response(200, {}, "the response")

        # Make a request and check the response.
        response = api.get_fulfillment_info("transaction ID")
        eq_("the response", response.content)

        # Verify that the 'HTTP request' was made to the right URL
        # with the right keyword arguments and the right HTTP method.
        url, args, kwargs = api.requests.pop()
        assert url.endswith(api.fulfillment_endpoint)
        eq_('POST', kwargs['method'])
        eq_('transaction ID', kwargs['params']['TransactionID'])


class TestCirculationMonitor(Axis360Test):

    BIBLIOGRAPHIC_DATA = Metadata(
        DataSource.AXIS_360,
        publisher=u'Random House Inc',
        language='eng',
        title=u'Faith of My Fathers : A Family Memoir',
        imprint=u'Random House Inc2',
        published=datetime.datetime(2000, 3, 7, 0, 0),
        primary_identifier=IdentifierData(
            type=Identifier.AXIS_360_ID,
            identifier=u'0003642860'
        ),
        identifiers = [
            IdentifierData(type=Identifier.ISBN, identifier=u'9780375504587')
        ],
        contributors = [
            ContributorData(sort_name=u"McCain, John",
                            roles=[Contributor.PRIMARY_AUTHOR_ROLE]
                        ),
            ContributorData(sort_name=u"Salter, Mark",
                            roles=[Contributor.AUTHOR_ROLE]
                        ),
        ],
        subjects = [
            SubjectData(type=Subject.BISAC,
                        identifier=u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
            SubjectData(type=Subject.FREEFORM_AUDIENCE,
                        identifier=u'Adult'),
        ],
    )

    AVAILABILITY_DATA = CirculationData(
        data_source=DataSource.AXIS_360,
        primary_identifier=BIBLIOGRAPHIC_DATA.primary_identifier,
        licenses_owned=9,
        licenses_available=8,
        licenses_reserved=0,
        patrons_in_hold_queue=0,
        last_checked=datetime.datetime(2015, 5, 20, 2, 9, 8),
    )

    def test_process_book(self):
        integration, ignore = create(
            self._db, ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        monitor = Axis360CirculationMonitor(
            self._db, self.collection, api_class=MockAxis360API,
        )
        edition, license_pool = monitor.process_book(
            self.BIBLIOGRAPHIC_DATA, self.AVAILABILITY_DATA)
        eq_(u'Faith of My Fathers : A Family Memoir', edition.title)
        eq_(u'eng', edition.language)
        eq_(u'Random House Inc', edition.publisher)
        eq_(u'Random House Inc2', edition.imprint)

        eq_(Identifier.AXIS_360_ID, edition.primary_identifier.type)
        eq_(u'0003642860', edition.primary_identifier.identifier)

        [isbn] = [x for x in edition.equivalent_identifiers()
                  if x is not edition.primary_identifier]
        eq_(Identifier.ISBN, isbn.type)
        eq_(u'9780375504587', isbn.identifier)

        eq_(["McCain, John", "Salter, Mark"],
            sorted([x.sort_name for x in edition.contributors]),
        )

        subs = sorted(
            (x.subject.type, x.subject.identifier)
            for x in edition.primary_identifier.classifications
        )
        eq_([(Subject.BISAC, u'BIOGRAPHY & AUTOBIOGRAPHY / Political'),
             (Subject.FREEFORM_AUDIENCE, u'Adult')], subs)

        eq_(9, license_pool.licenses_owned)
        eq_(8, license_pool.licenses_available)
        eq_(0, license_pool.patrons_in_hold_queue)
        eq_(datetime.datetime(2015, 5, 20, 2, 9, 8), license_pool.last_checked)

        # Three circulation events were created, backdated to the
        # last_checked date of the license pool.
        events = license_pool.circulation_events
        eq_([u'distributor_title_add', u'distributor_check_in', u'distributor_license_add'],
            [x.type for x in events])
        for e in events:
            eq_(e.start, license_pool.last_checked)

        # A presentation-ready work has been created for the LicensePool.
        work = license_pool.work
        eq_(True, work.presentation_ready)
        eq_("Faith of My Fathers : A Family Memoir", work.title)

        # A CoverageRecord has been provided for this book in the Axis
        # 360 bibliographic coverage provider, so that in the future
        # it doesn't have to make a separate API request to ask about
        # this book.
        records = [x for x in license_pool.identifier.coverage_records
                   if x.data_source.name == DataSource.AXIS_360
                   and x.operation is None]
        eq_(1, len(records))

    def test_process_book_updates_old_licensepool(self):
        """If the LicensePool already exists, the circulation monitor
        updates it.
        """
        edition, licensepool = self._edition(
            with_license_pool=True, identifier_type=Identifier.AXIS_360_ID,
            identifier_id=u'0003642860'
        )
        # We start off with availability information based on the
        # default for test data.
        eq_(1, licensepool.licenses_owned)

        identifier = IdentifierData(
            type=licensepool.identifier.type,
            identifier=licensepool.identifier.identifier
        )
        metadata = Metadata(DataSource.AXIS_360, primary_identifier=identifier)
        monitor = Axis360CirculationMonitor(
            self._db, self.collection, api_class=MockAxis360API,
        )
        edition, licensepool = monitor.process_book(
            metadata, self.AVAILABILITY_DATA
        )

        # Now we have information based on the CirculationData.
        eq_(9, licensepool.licenses_owned)


class TestReaper(Axis360Test):

    def test_instantiate(self):
        # Validate the standard CollectionMonitor interface.
        monitor = AxisCollectionReaper(
            self._db, self.collection,
            api_class=MockAxis360API
        )

class TestParsers(Axis360Test):

    def test_bibliographic_parser(self):
        """Make sure the bibliographic information gets properly
        collated in preparation for creating Edition objects.
        """
        data = self.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            False, True).process_all(data)

        # We didn't ask for availability information, so none was provided.
        eq_(None, av1)
        eq_(None, av2)

        eq_(u'Faith of My Fathers : A Family Memoir', bib1.title)
        eq_('eng', bib1.language)
        eq_(datetime.datetime(2000, 3, 7, 0, 0), bib1.published)

        eq_(u'Simon & Schuster', bib2.publisher)
        eq_(u'Pocket Books', bib2.imprint)

        eq_(Edition.BOOK_MEDIUM, bib1.medium)

        # TODO: Would be nicer if we could test getting a real value
        # for this.
        eq_(None, bib2.series)

        # Book #1 has a description.
        [description] = bib1.links
        eq_(Hyperlink.DESCRIPTION, description.rel)
        eq_(Representation.TEXT_PLAIN, description.media_type)
        assert description.content.startswith(
            "John McCain's deeply moving memoir"
        )

        # Book #1 has a primary author, another author and a narrator.
        #
        # TODO: The narrator data is simulated. we haven't actually
        # verified that Axis 360 sends narrator information in the
        # same format as author information.
        [cont1, cont2, narrator] = bib1.contributors
        eq_("McCain, John", cont1.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont1.roles)

        eq_("Salter, Mark", cont2.sort_name)
        eq_([Contributor.AUTHOR_ROLE], cont2.roles)

        eq_("McCain, John S. III", narrator.sort_name)
        eq_([Contributor.NARRATOR_ROLE], narrator.roles)

        # Book #2 only has a primary author.
        [cont] = bib2.contributors
        eq_("Pollero, Rhonda", cont.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], cont.roles)

        axis_id, isbn = sorted(bib1.identifiers, key=lambda x: x.identifier)
        eq_(u'0003642860', axis_id.identifier)
        eq_(u'9780375504587', isbn.identifier)

        # Check the subjects for #2 because it includes an audience,
        # unlike #1.
        subjects = sorted(bib2.subjects, key = lambda x: x.identifier)
        eq_([Subject.BISAC, Subject.BISAC, Subject.BISAC,
             Subject.AXIS_360_AUDIENCE], [x.type for x in subjects])
        general_fiction, women_sleuths, romantic_suspense = sorted([
            x.name for x in subjects if x.type==Subject.BISAC])
        eq_(u'FICTION / General', general_fiction)
        eq_(u'FICTION / Mystery & Detective / Women Sleuths', women_sleuths)
        eq_(u'FICTION / Romance / Suspense', romantic_suspense)

        [adult] = [x.identifier for x in subjects
                   if x.type==Subject.AXIS_360_AUDIENCE]
        eq_(u'General Adult', adult)

        '''
        TODO:  Perhaps want to test formats separately.
        [format] = bib1.formats
        eq_(Representation.EPUB_MEDIA_TYPE, format.content_type)
        eq_(DeliveryMechanism.ADOBE_DRM, format.drm_scheme)

        # The second book is only available in 'Blio' format, which
        # we can't use.
        eq_([], bib2.formats)
        '''

    def test_bibliographic_parser_audiobook(self):
        # TODO - we need a real example to test from.
        # Verify narrator information and medium.
        pass

    def test_parse_author_role(self):
        """Suffixes on author names are turned into roles."""
        author = "Dyssegaard, Elisabeth Kallick (TRN)"
        parse = BibliographicParser.parse_contributor
        c = parse(author)
        eq_("Dyssegaard, Elisabeth Kallick", c.sort_name)
        eq_([Contributor.TRANSLATOR_ROLE], c.roles)

        # A corporate author is given a normal author role.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.PRIMARY_AUTHOR_ROLE], c.roles)

        c = parse(author, primary_author_found=True)
        eq_("Bob, Inc.", c.sort_name)
        eq_([Contributor.AUTHOR_ROLE], c.roles)

        # An unknown author type is given an unknown role
        author = "Eve, Mallory (ZZZ)"
        c = parse(author, primary_author_found=False)
        eq_("Eve, Mallory", c.sort_name)
        eq_([Contributor.UNKNOWN_ROLE], c.roles)

        # force_role overwrites whatever other role might be
        # assigned.
        author = "Bob, Inc. (COR)"
        c = parse(author, primary_author_found=False,
                  force_role=Contributor.NARRATOR_ROLE)
        eq_([Contributor.NARRATOR_ROLE], c.roles)


    def test_availability_parser(self):
        """Make sure the availability information gets properly
        collated in preparation for updating a LicensePool.
        """

        data = self.sample_data("tiny_collection.xml")

        [bib1, av1], [bib2, av2] = BibliographicParser(
            True, False).process_all(data)

        # We didn't ask for bibliographic information, so none was provided.
        eq_(None, bib1)
        eq_(None, bib2)

        eq_("0003642860", av1.primary_identifier(self._db).identifier)
        eq_(9, av1.licenses_owned)
        eq_(9, av1.licenses_available)
        eq_(0, av1.patrons_in_hold_queue)


class TestResponseParser(object):

    @classmethod
    def sample_data(self, filename):
        return sample_data(filename, 'axis')

    def setup(self):
        # We don't need an actual Collection object to test this
        # class, but we do need to test that whatever object we
        # _claim_ is a Collection will have its id put into the
        # right spot of HoldInfo and LoanInfo objects.
        class MockCollection(object):
            pass
        self._default_collection = MockCollection()
        self._default_collection.id = object()

class TestRaiseExceptionOnError(TestResponseParser):

    def test_internal_server_error(self):
        data = self.sample_data("internal_server_error.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "Internal Server Error",
            parser.process_all, data
        )

    def test_internal_server_error(self):
        data = self.sample_data("invalid_error_code.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "Invalid response code from Axis 360: abcd",
            parser.process_all, data
        )

    def test_missing_error_code(self):
        data = self.sample_data("missing_error_code.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises_regexp(
            RemoteInitiatedServerError, "No status code!",
            parser.process_all, data
        )


class TestCheckoutResponseParser(TestResponseParser):

    def test_parse_checkout_success(self):
        data = self.sample_data("checkout_success.xml")
        parser = CheckoutResponseParser(self._default_collection)
        parsed = parser.process_all(data)
        assert isinstance(parsed, LoanInfo)
        eq_(self._default_collection.id, parsed.collection_id)
        eq_(DataSource.AXIS_360, parsed.data_source_name)
        eq_(Identifier.AXIS_360_ID, parsed.identifier_type)
        eq_(datetime.datetime(2015, 8, 11, 18, 57, 42),
            parsed.end_date)

        assert isinstance(parsed.fulfillment_info, FulfillmentInfo)
        eq_("http://axis360api.baker-taylor.com/Services/VendorAPI/GetAxisDownload/v2?blahblah",
            parsed.fulfillment_info.content_link)

    def test_parse_already_checked_out(self):
        data = self.sample_data("already_checked_out.xml")
        parser = CheckoutResponseParser(None)
        assert_raises(AlreadyCheckedOut, parser.process_all, data)

    def test_parse_not_found_on_remote(self):
        data = self.sample_data("not_found_on_remote.xml")
        parser = CheckoutResponseParser(None)
        assert_raises(NotFoundOnRemote, parser.process_all, data)

class TestHoldResponseParser(TestResponseParser):

    def test_parse_hold_success(self):
        data = self.sample_data("place_hold_success.xml")
        parser = HoldResponseParser(self._default_collection)
        parsed = parser.process_all(data)
        assert isinstance(parsed, HoldInfo)
        eq_(1, parsed.hold_position)

        # The HoldInfo is given the Collection object we passed into
        # the HoldResponseParser.
        eq_(self._default_collection.id, parsed.collection_id)

    def test_parse_already_on_hold(self):
        data = self.sample_data("already_on_hold.xml")
        parser = HoldResponseParser(None)
        assert_raises(AlreadyOnHold, parser.process_all, data)

class TestHoldReleaseResponseParser(TestResponseParser):

    def test_success(self):
        data = self.sample_data("release_hold_success.xml")
        parser = HoldReleaseResponseParser(None)
        eq_(True, parser.process_all(data))

    def test_failure(self):
        data = self.sample_data("release_hold_failure.xml")
        parser = HoldReleaseResponseParser(None)
        assert_raises(NotOnHold, parser.process_all, data)

class TestAvailabilityResponseParser(TestResponseParser):

    def test_parse_loan_and_hold(self):
        data = self.sample_data("availability_with_loan_and_hold.xml")
        parser = AvailabilityResponseParser(self._default_collection)
        activity = list(parser.process_all(data))
        hold, loan, reserved = sorted(activity, key=lambda x: x.identifier)
        eq_(self._default_collection.id, hold.collection_id)
        eq_(Identifier.AXIS_360_ID, hold.identifier_type)
        eq_("0012533119", hold.identifier)
        eq_(1, hold.hold_position)
        eq_(None, hold.end_date)

        eq_(self._default_collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_("http://fulfillment/", loan.fulfillment_info.content_link)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

        eq_(self._default_collection.id, reserved.collection_id)
        eq_("1111111111", reserved.identifier)
        eq_(datetime.datetime(2015, 1, 1, 13, 11, 11), reserved.end_date)
        eq_(0, reserved.hold_position)

    def test_parse_loan_no_availability(self):
        data = self.sample_data("availability_without_fulfillment.xml")
        parser = AvailabilityResponseParser(self._default_collection)
        [loan] = list(parser.process_all(data))

        eq_(self._default_collection.id, loan.collection_id)
        eq_("0015176429", loan.identifier)
        eq_(None, loan.fulfillment_info)
        eq_(datetime.datetime(2015, 8, 12, 17, 40, 27), loan.end_date)

    def test_parse_audiobook_fulfillmentinfo(self):
        data = self.sample_data("audiobook_fulfillment_info.json")
        parser = AvailabilityResponseParser(self._default_collection)
        [loan] = list(parser.process_all(data))
        fulfillment = loan.fulfillment_info
        assert isinstance(fulfillment, AudiobookFufillmentInfo)

        # The transaction ID is stored as the .key. If we actually
        # need to make a manifest for this audiobook, the key will be
        # used in one more API request. (See TestAudiobookFulfillmentInfo
        # for that.)
        eq_("C3F71F8D-1883-2B34-068F-96570678AEB0", fulfillment.key)


class TestFulfillmentInfoResponseParser(Axis360Test):

    def test_parse(self):
        # Verify that parse() parses a JSON document
        # and calls _extract() on it.
        class Mock(FulfillmentInfoResponseParser):
            def _extract(self, license_pool, parsed):
                self.called_with = (license_pool, parsed)
                return "Extracted document"

        parser = Mock(self._default_collection)
        license_pool = object()

        # This works whether we pass it a JSON document or the object
        # resulting from parsing such a document.
        for json_document in ("{}", {}):
            result = parser.parse(license_pool, "{}")
            eq_("Extracted document", result)
            eq_((license_pool, {}), parser.called_with)
            parser.called_with = None

        # Any other input causes an error.
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Invalid response from Axis 360 \(was expecting JSON\): I'm not JSON",
            parser.parse, license_pool, "I'm not JSON"
        )

    def test__required_key(self):
        # Test the _required_key helper method.
        m = FulfillmentInfoResponseParser._required_key
        eq_("value", m("thekey", dict(thekey="value")))

        for bad_dict in (None, {}, dict(otherkey="value")):
            assert_raises_regexp(
                RemoteInitiatedServerError,
                "Required key thekey not present in Axis 360 fulfillment document",
                m, "thekey", bad_dict
            )

    def test__extract(self):
        # _extract will create a valid FindawayManifest given a
        # complete document.

        m = FulfillmentInfoResponseParser._extract

        edition, pool = self._edition(with_license_pool=True)
        parser = (self._default_collection)
        def get_data():
            # We'll be modifying this document to simulate failures,
            # so make it easy to load a fresh copy.
            return json.loads(
                self.sample_data("audiobook_fulfillment_info.json")
            )
        data = get_data()
        manifest, expires = m(pool, data)

        assert isinstance(manifest, FindawayManifest)
        metadata = manifest.metadata

        # The manifest contains information from the LicensePool's presentation
        # edition
        eq_(edition.title, metadata['title'])

        # It contains DRM licensing information from Findaway via the
        # Axis 360 API.
        encrypted = metadata['encrypted']
        eq_(
            '0f547af1-38c1-4b1c-8a1a-169d353065d0',
            encrypted['findaway:sessionKey']
        )
        eq_('5babb89b16a4ed7d8238f498', encrypted['findaway:checkoutId'])
        eq_('04960', encrypted['findaway:fulfillmentId'])
        eq_('58ee81c6d3d8eb3b05597cdc', encrypted['findaway:licenseId'])

        # There are no spine items and the book is of unknown duration.
        assert 'duration' not in metadata
        eq_([], manifest.readingOrder)

        # We also know when the licensing document expires.
        eq_(datetime.datetime(2018, 9, 29, 18, 34), expires)

        # Now strategically remove required information from the
        # document and verify that extraction fails.
        #
        no_status_code = get_data()
        del no_status_code['Status']['Code']
        assert_raises_regexp(
            RemoteInitiatedServerError, "Required key Code not present",
            m, pool, no_status_code
        )

        no_status = get_data()
        del no_status['Status']
        assert_raises_regexp(
            RemoteInitiatedServerError, "Required key Status not present",
            m, pool, no_status
        )

        for field in ('Status', 'FNDLicenseID', 'FNDSessionKey', 'ExpirationDate'):
            missing_field = get_data()
            del missing_field[field]
            assert_raises_regexp(
                RemoteInitiatedServerError,
                "Required key %s not present" % field,
                m, pool, missing_field
            )

        # Try with a bad expiration date.
        bad_date = get_data()
        bad_date['ExpirationDate'] = 'not-a-date'
        assert_raises_regexp(
            RemoteInitiatedServerError,
            "Could not parse expiration date: not-a-date",
            m, pool, bad_date
        )


class TestAxis360BibliographicCoverageProvider(Axis360Test):
    """Test the code that looks up bibliographic information from Axis 360."""

    def setup(self):
        super(TestAxis360BibliographicCoverageProvider, self).setup()
        self.provider = Axis360BibliographicCoverageProvider(
            self.collection, api_class=MockAxis360API
        )
        self.api = self.provider.api

    def test_script_instantiation(self):
        """Test that RunCoverageProviderScript can instantiate
        the coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            Axis360BibliographicCoverageProvider, self._db,
            api_class=MockAxis360API
        )
        [provider] = script.providers
        assert isinstance(provider, Axis360BibliographicCoverageProvider)
        assert isinstance(provider.api, MockAxis360API)

    def test_process_item_creates_presentation_ready_work(self):
        """Test the normal workflow where we ask Axis for data,
        Axis provides it, and we create a presentation-ready work.
        """
        data = self.sample_data("single_item.xml")
        self.api.queue_response(200, content=data)

        # Here's the book mentioned in single_item.xml.
        identifier = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = '0003642860'

        # This book has no LicensePool.
        eq_([], identifier.licensed_through)

        # Run it through the Axis360BibliographicCoverageProvider
        [result] = self.provider.process_batch([identifier])
        eq_(identifier, result)

        # A LicensePool was created. We know both how many copies of this
        # book are available, and what formats it's available in.
        [pool] = identifier.licensed_through
        eq_(9, pool.licenses_owned)
        [lpdm] = pool.delivery_mechanisms
        eq_('application/epub+zip (application/vnd.adobe.adept+xml)',
            lpdm.delivery_mechanism.name)

        # A Work was created and made presentation ready.
        eq_('Faith of My Fathers : A Family Memoir', pool.work.title)
        eq_(True, pool.work.presentation_ready)

    def test_transient_failure_if_requested_book_not_mentioned(self):
        """Test an unrealistic case where we ask Axis 360 about one book and
        it tells us about a totally different book.
        """
        # We're going to ask about abcdef
        identifier = self._identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = 'abcdef'

        # But we're going to get told about 0003642860.
        data = self.sample_data("single_item.xml")
        self.api.queue_response(200, content=data)

        [result] = self.provider.process_batch([identifier])

        # Coverage failed for the book we asked about.
        assert isinstance(result, CoverageFailure)
        eq_(identifier, result.obj)
        eq_("Book not in collection", result.exception)

        # And nothing major was done about the book we were told
        # about. We created an Identifier record for its identifier,
        # but no LicensePool or Edition.
        wrong_identifier = Identifier.for_foreign_id(
            self._db, Identifier.AXIS_360_ID, "0003642860"
        )
        eq_([], identifier.licensed_through)
        eq_([], identifier.primarily_identifies)
