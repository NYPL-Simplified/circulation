import datetime
import json
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from . import DatabaseTest, sample_data

from core.metadata_layer import Metadata
from core.model import (
    get_one,
    get_one_or_create,
    DataSource,
    Edition,
    ExternalIntegration,
    Identifier,
    Representation,
)
from api.novelist import (
    MockNoveListAPI,
    NoveListAPI,
    NoveListCoverageProvider,
)
from core.util.http import (
    HTTP
)
from core.testing import MockRequestsResponse


class TestNoveListAPI(DatabaseTest):
    """Tests the NoveList API service object"""

    def setup(self):
        super(TestNoveListAPI, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.NOVELIST,
            ExternalIntegration.METADATA_GOAL, username=u'library',
            password=u'yep', libraries=[self._default_library],
        )
        self.novelist = NoveListAPI.from_config(self._default_library)

    def teardown(self):
        NoveListAPI.IS_CONFIGURED = None
        super(TestNoveListAPI, self).teardown()

    def sample_data(self, filename):
        return sample_data(filename, 'novelist')

    def sample_representation(self, filename):
        content = self.sample_data(filename)
        return self._representation(
            media_type='application/json', content=content
        )[0]

    def test_from_config(self):
        """Confirms that NoveListAPI can be built from config successfully"""
        novelist = NoveListAPI.from_config(self._default_library)
        eq_(True, isinstance(novelist, NoveListAPI))
        eq_("library", novelist.profile)
        eq_("yep", novelist.password)

        # Without either configuration value, an error is raised.
        self.integration.password = None
        assert_raises(ValueError, NoveListAPI.from_config, self._default_library)

        self.integration.password = u'yep'
        self.integration.username = None
        assert_raises(ValueError, NoveListAPI.from_config, self._default_library)

    def test_is_configured(self):
        # If an ExternalIntegration exists, the API is_configured
        eq_(True, NoveListAPI.is_configured(self._default_library))
        # A class variable is set to reduce future database requests.
        eq_(self._default_library.id, NoveListAPI._configuration_library_id)

        # If an ExternalIntegration doesn't exist for the library, it is not.
        library = self._library()
        eq_(False, NoveListAPI.is_configured(library))
        # And the class variable is updated.
        eq_(library.id, NoveListAPI._configuration_library_id)

    def test_review_response(self):
        invalid_credential_response = (403, {}, 'HTML Access Denied page')
        assert_raises(Exception, self.novelist.review_response, invalid_credential_response)

        missing_argument_response = (200, {}, '"Missing ISBN, UPC, or Client Identifier!"')
        assert_raises(Exception, self.novelist.review_response, missing_argument_response)

        response = (200, {}, "Here's the goods!")
        eq_(response, self.novelist.review_response(response))

    def test_lookup_info_to_metadata(self):
        # Basic book information is returned
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.ISBN, "9780804171335"
        )
        bad_character = self.sample_representation("a_bad_character.json")
        metadata = self.novelist.lookup_info_to_metadata(bad_character)

        eq_(True, isinstance(metadata, Metadata))
        eq_(Identifier.NOVELIST_ID, metadata.primary_identifier.type)
        eq_('10392078', metadata.primary_identifier.identifier)
        eq_("A bad character", metadata.title)
        eq_(None, metadata.subtitle)
        eq_(1, len(metadata.contributors))
        [contributor] = metadata.contributors
        eq_("Kapoor, Deepti", contributor.sort_name)
        eq_(4, len(metadata.identifiers))
        eq_(4, len(metadata.subjects))
        eq_(2, len(metadata.measurements))
        ratings = sorted(metadata.measurements, key=lambda m: m.value)
        eq_(2, ratings[0].value)
        eq_(3.27, ratings[1].value)
        eq_(625, len(metadata.recommendations))

        # Confirm that Lexile and series data is extracted with a
        # different sample.
        vampire = self.sample_representation("vampire_kisses.json")
        metadata = self.novelist.lookup_info_to_metadata(vampire)

        [lexile] = filter(lambda s: s.type=='Lexile', metadata.subjects)
        eq_(u'630', lexile.identifier)
        eq_(u'Vampire kisses manga', metadata.series)
        # The full title should be selected, since every volume
        # has the same main title: 'Vampire kisses'
        eq_(u'Vampire kisses: blood relatives. Volume 1', metadata.title)
        eq_(1, metadata.series_position)
        eq_(5, len(metadata.recommendations))

    def test_get_series_information(self):

        metadata = Metadata(data_source=DataSource.NOVELIST)
        vampire = json.loads(self.sample_data("vampire_kisses.json"))
        book_info = vampire['TitleInfo']
        series_info = vampire['FeatureContent']['SeriesInfo']

        (metadata, ideal_title_key) = self.novelist.get_series_information(
            metadata, series_info, book_info
        )
        # Relevant series information is extracted
        eq_('Vampire kisses manga', metadata.series)
        eq_(1, metadata.series_position)
        # The 'full_title' key should be returned as ideal because
        # all the volumes have the same 'main_title'
        eq_('full_title', ideal_title_key)


        watchman = json.loads(self.sample_data("alternate_series_example.json"))
        book_info = watchman['TitleInfo']
        series_info = watchman['FeatureContent']['SeriesInfo']
        # Confirms that the new example doesn't match any volume's full title
        eq_([], [v for v in series_info['series_titles']
                if v.get('full_title')==book_info.get('full_title')])

        # But it still finds its matching volume
        (metadata, ideal_title_key) = self.novelist.get_series_information(
            metadata, series_info, book_info
        )
        eq_('Elvis Cole/Joe Pike novels', metadata.series)
        eq_(11, metadata.series_position)
        # And recommends using the main_title
        eq_('main_title', ideal_title_key)

        # If the volume is found in the series more than once...
        book_info = dict(
            main_title='The Baby-Sitters Club',
            full_title='The Baby-Sitters Club: Claudia and Mean Janine'
        )
        series_info = dict(
            full_title='The Baby-Sitters Club series',
            series_titles=[
                # The volume is here twice!
                book_info,
                book_info,
                dict(
                    full_title='The Baby-Sitters Club',
                    main_title='The Baby-Sitters Club: Claudia and Mean Janine',
                    series_position='3.'
                )
            ]
        )
        # An error is raised.
        assert_raises(
            ValueError, self.novelist.get_series_information,
            metadata, series_info, book_info
        )

    def test_lookup_info_to_metadata_ignores_empty_responses(self):
        """API requests that return no data result return a None tuple"""

        null_response = self.sample_representation("null_data.json")
        result = self.novelist.lookup_info_to_metadata(null_response)
        eq_(None, result)

        # This also happens when NoveList indicates with an empty
        # response that it doesn't know the ISBN.
        empty_response = self.sample_representation("unknown_isbn.json")
        result = self.novelist.lookup_info_to_metadata(empty_response)
        eq_(None, result)

    def test_build_query_url(self):
        params = dict(
            ClientIdentifier='C I',
            ISBN='456',
            version='2.2',
            profile='username',
            password='secret'
        )

        # Authentication information is included in the URL by default
        full_result = self.novelist.build_query_url(params)
        auth_details = '&profile=username&password=secret'
        eq_(True, full_result.endswith(auth_details))
        assert 'profile=username' in full_result
        assert 'password=secret' in full_result

        # With a scrub, no authentication information is included.
        scrubbed_result = self.novelist.build_query_url(params, include_auth=False)
        eq_(False, scrubbed_result.endswith(auth_details))
        assert 'profile=username' not in scrubbed_result
        assert 'password=secret' not in scrubbed_result

        # Other details are urlencoded and available in both versions.
        for url in (scrubbed_result, full_result):
            assert 'ClientIdentifier=C%20I' in url
            assert 'ISBN=456' in url
            assert 'version=2.2' in url

        # The method to create a scrubbed url returns the same result
        # as the NoveListAPI.build_query_url
        eq_(scrubbed_result, self.novelist.scrubbed_url(params))

    def test_cached_representation(self):
        url = self._url

        # If there's no Representation, nothing is returned.
        result = self.novelist.cached_representation(url)
        eq_(None, result)

        # If a recent Representation exists, it is returned.
        representation, is_new = self._representation(url=url)
        representation.content = 'content'
        representation.fetched_at = datetime.datetime.utcnow() - datetime.timedelta(days=3)
        result = self.novelist.cached_representation(url)
        eq_(representation, result)

        # If an old Representation exists, it's deleted.
        representation.fetched_at = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        result = self.novelist.cached_representation(url)
        eq_(None, result)
        self._db.commit()
        assert representation not in self._db

    def test_scrub_subtitle(self):
        """Unnecessary title segments are removed from subtitles"""

        scrub = self.novelist._scrub_subtitle
        eq_(None, scrub(None))
        eq_(None, scrub('[electronic resource]'))
        eq_(None, scrub('[electronic resource] :  '))
        eq_('A Biomythography', scrub('[electronic resource] :  A Biomythography'))

    def test_confirm_same_identifier(self):
        source = DataSource.lookup(self._db, DataSource.NOVELIST)
        identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, '84752928'
        )
        unmatched_identifier, ignore = Identifier.for_foreign_id(
            self._db, Identifier.NOVELIST_ID, '23781947'
        )
        metadata = Metadata(source, primary_identifier=identifier)
        match = Metadata(source, primary_identifier=identifier)
        mistake = Metadata(source, primary_identifier=unmatched_identifier)

        eq_(False, self.novelist._confirm_same_identifier([metadata, mistake]))
        eq_(True, self.novelist._confirm_same_identifier([metadata, match]))

    def test_lookup_equivalent_isbns(self):
        identifier = self._identifier(identifier_type=Identifier.OVERDRIVE_ID)
        api = MockNoveListAPI.from_config(self._default_library)

        # If there are no ISBN equivalents, it returns None.
        eq_(None, api.lookup_equivalent_isbns(identifier))

        source = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        identifier.equivalent_to(source, self._identifier(), strength=1)
        self._db.commit()
        eq_(None, api.lookup_equivalent_isbns(identifier))

        # If there's an ISBN equivalent, but it doesn't result in metadata,
        # it returns none.
        isbn = self._identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn, strength=1)
        self._db.commit()
        api.responses.append(None)
        eq_(None, api.lookup_equivalent_isbns(identifier))

        # Create an API class that can mockout NoveListAPI.choose_best_metadata
        class MockBestMetadataAPI(MockNoveListAPI):
            choose_best_metadata_return = None
            def choose_best_metadata(self, *args, **kwargs):
                return self.choose_best_metadata_return
        api = MockBestMetadataAPI.from_config(self._default_library)

        # Give the identifier another ISBN equivalent.
        isbn2 = self._identifier(identifier_type=Identifier.ISBN)
        identifier.equivalent_to(source, isbn2, strength=1)
        self._db.commit()

        # Queue metadata responses for each ISBN lookup.
        metadatas = [object(), object()]
        api.responses.extend(metadatas)

        # If choose_best_metadata returns None, the lookup returns None.
        api.choose_best_metadata_return = (None, None)
        eq_(None, api.lookup_equivalent_isbns(identifier))

        # Lookup was performed for both ISBNs.
        eq_([], api.responses)

        # If choose_best_metadata returns a low confidence metadata, the
        # lookup returns None.
        api.responses.extend(metadatas)
        api.choose_best_metadata_return = (metadatas[0], 0.33)
        eq_(None, api.lookup_equivalent_isbns(identifier))

        # If choose_best_metadata returns a high confidence metadata, the
        # lookup returns the metadata.
        api.responses.extend(metadatas)
        api.choose_best_metadata_return = (metadatas[1], 0.67)
        eq_(metadatas[1], api.lookup_equivalent_isbns(identifier))

    def test_choose_best_metadata(self):
        more_identifier = self._identifier(identifier_type=Identifier.NOVELIST_ID)
        less_identifier = self._identifier(identifier_type=Identifier.NOVELIST_ID)
        metadatas = [Metadata(DataSource.NOVELIST, primary_identifier=more_identifier)]

        # When only one Metadata object is given, that object is returned.
        result = self.novelist.choose_best_metadata(metadatas, self._identifier())
        eq_(True, isinstance(result, tuple))
        eq_(metadatas[0], result[0])
        # A default confidence of 1.0 is returned.
        eq_(1.0, result[1])

        # When top identifiers have equal representation, the method returns none.
        metadatas.append(Metadata(DataSource.NOVELIST, primary_identifier=less_identifier))
        eq_((None, None), self.novelist.choose_best_metadata(metadatas, self._identifier()))

        # But when one pulls ahead, we get the metadata object again.
        metadatas.append(Metadata(DataSource.NOVELIST, primary_identifier=more_identifier))
        result = self.novelist.choose_best_metadata(metadatas, self._identifier())
        eq_(True, isinstance(result, tuple))
        metadata, confidence = result
        eq_(True, isinstance(metadata, Metadata))
        eq_(0.67, round(confidence, 2))
        eq_(more_identifier, metadata.primary_identifier)

    def test_get_items_from_query(self):
        items = self.novelist.get_items_from_query(self._default_library)
        eq_(items, [])

        edition = self._edition(identifier_type=Identifier.ISBN)
        pool = self._licensepool(edition, collection=self._default_collection)
        contributor = self._contributor(sort_name=edition.sort_author, name=edition.author)

        items = self.novelist.get_items_from_query(self._default_library)

        item = dict(
            Author=contributor[0]._sort_name,
            Title=edition.title,
            MediaType=self.novelist.medium_to_book_format_type_values.get(edition.medium, ""),
            ISBN=edition.primary_identifier.identifier,
            Narrator=""
        )

        eq_(items, [item])

    def test_create_item_object(self):
        (currentIdentifier, existingItem, newItem, addItem) = self.novelist.create_item_object(None, None, None)
        eq_(currentIdentifier, None)
        eq_(existingItem, None)
        eq_(newItem, None)
        eq_(addItem, False)

        # Item row from the db query
        # (identifier, identifier type, identifier,
        # edition title, edition medium,
        # contribution role, contributor sort name)
        item_from_query = (
            "12345", "Axis 360 ID", "23456",
            "Title 1", "Book",
            "Author", "Author 1")
        second_item_from_query = (
            "12345", "Axis 360 ID", "23456",
            "Title 1", "Book",
            "Primary Author", "Author 2")
        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(item_from_query, None, None)
        )
        eq_(currentIdentifier, item_from_query[2])
        eq_(existingItem, None)
        eq_(
            newItem,
            {"ISBN": "23456",
            "MediaType": "EBook",
            "Title": "Title 1",
            "Role": "Author",
            "Author": "Author 1",
            "Narrator": ""}
        )
        eq_(addItem, True)

        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(second_item_from_query, second_item_from_query[2], newItem)
        )
        eq_(currentIdentifier, item_from_query[2])
        eq_(existingItem,
            {"ISBN": "23456",
            "MediaType": "EBook",
            "Title": "Title 1",
            "Author": "Author 2",
            "Role": "Primary Author",
            "Narrator": ""}
        )
        eq_(newItem, None)
        eq_(addItem, False)

    def test_put_items_novelist(self):
        response = self.novelist.put_items_novelist(self._default_library)

        eq_(response, None)

        edition = self._edition(identifier_type=Identifier.ISBN)
        pool = self._licensepool(edition, collection=self._default_collection)
        mock_response = {'Customer': 'NYPL', 'RecordsReceived': 10}

        def mockHTTPPut(url, headers, **kwargs):
            return MockRequestsResponse(200, content=json.dumps(mock_response))

        oldPut = self.novelist.put
        self.novelist.put = mockHTTPPut

        response = self.novelist.put_items_novelist(self._default_library)

        eq_(response, mock_response)

        self.novelist.put = oldPut

    def test_make_novelist_data_object(self):
        bad_data = []
        result = self.novelist.make_novelist_data_object(bad_data)

        eq_(result, {
            "Customer": "library:yep",
            "Records": []
        })

        data = [
            {"ISBN":"12345", "MediaType": "http://schema.org/EBook", "Title": "Book 1", "Author": "Author 1" },
            {"ISBN":"12346", "MediaType": "http://schema.org/EBook", "Title": "Book 2", "Author": "Author 2" },
        ]
        result = self.novelist.make_novelist_data_object(data)

        eq_(result, {
            "Customer": "library:yep",
            "Records": data
        })

    def mockHTTPPut(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_put(self):
        oldPut = HTTP.put_with_timeout

        HTTP.put_with_timeout = self.mockHTTPPut

        headers = {"AuthorizedIdentifier": "authorized!"}
        isbns = ["12345", "12346", "12347"]
        data = self.novelist.make_novelist_data_object(isbns)

        response = self.novelist.put("http://apiendpoint.com", headers, data=data)
        (params, args) = self.called_with

        eq_(params, ("http://apiendpoint.com", data))
        eq_(args["headers"], headers)

        HTTP.put_with_timeout = oldPut


class TestNoveListCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestNoveListCoverageProvider, self).setup()
        self.integration = self._external_integration(
            ExternalIntegration.NOVELIST,
            ExternalIntegration.METADATA_GOAL, username=u'library',
            password=u'yep', libraries=[self._default_library]
        )

        self.novelist = NoveListCoverageProvider(self._db)
        self.novelist.api = MockNoveListAPI.from_config(self._default_library)

        self.metadata = Metadata(
            data_source = self.novelist.data_source,
            primary_identifier=self._identifier(
                identifier_type=Identifier.NOVELIST_ID
            ),
            title=u"The Great American Novel"
        )

    def test_process_item(self):
        identifier = self._identifier()
        self.novelist.api.setup(None, self.metadata)

        # When the response is None, the identifier is returned.
        eq_(identifier, self.novelist.process_item(identifier))

        # When the response is a Metadata object, the identifiers are set
        # as equivalent and the metadata identifier's edition is updated.
        eq_(identifier, self.novelist.process_item(identifier))
        [edition] = self.metadata.primary_identifier.primarily_identifies
        eq_(u"The Great American Novel", edition.title)
        equivalents = [eq.output for eq in identifier.equivalencies]
        eq_(True, self.metadata.primary_identifier in equivalents)

    def test_process_item_creates_edition_for_series_info(self):
        work = self._work(with_license_pool=True)
        identifier = work.license_pools[0].identifier

        # Without series information, a NoveList-source edition is not
        # created for the original identifier.
        self.metadata.series = self.metadata.series_position = None
        self.novelist.api.setup(self.metadata)
        eq_(identifier, self.novelist.process_item(identifier))
        novelist_edition = get_one(
            self._db, Edition, data_source=self.novelist.data_source,
            primary_identifier=identifier
        )
        eq_(None, novelist_edition)

        # When series information exists, an edition is created for the
        # licensed identifier.
        self.metadata.series = "A Series of Unfortunate Events"
        self.metadata.series_position = 6
        self.novelist.api.setup(self.metadata)
        self.novelist.process_item(identifier)
        novelist_edition = get_one(
            self._db, Edition, data_source=self.novelist.data_source,
            primary_identifier=identifier
        )
        assert novelist_edition
        eq_(self.metadata.series, novelist_edition.series)
        eq_(self.metadata.series_position, novelist_edition.series_position)
        # Other basic metadata is also stored.
        eq_(self.metadata.title, novelist_edition.title)
