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
from core.testing import DummyHTTPClient
from api.config import CannotLoadConfiguration
from api.novelist import (
    MockNoveListAPI,
    NoveListAPI,
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
        assert_raises(CannotLoadConfiguration, NoveListAPI.from_config, self._default_library)

        self.integration.password = u'yep'
        self.integration.username = None
        assert_raises(CannotLoadConfiguration, NoveListAPI.from_config, self._default_library)

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

    def test_lookup(self):
        # Test the lookup() method.
        h = DummyHTTPClient()
        h.queue_response(200, "text/html", content="yay")

        class Mock(NoveListAPI):
            def build_query_url(self, params):
                self.build_query_url_called_with = params
                return "http://query-url/"

            def scrubbed_url(self, params):
                self.scrubbed_url_called_with = params
                return "http://scrubbed-url/"

            def review_response(self, response):
                self.review_response_called_with = response

            def lookup_info_to_metadata(self, representation):
                self.lookup_info_to_metadata_called_with = representation
                return "some metadata"

        novelist = Mock.from_config(self._default_library)
        identifier = self._identifier(identifier_type=Identifier.ISBN)

        # Do the lookup.
        result = novelist.lookup(identifier, do_get=h.do_get)

        # A number of parameters were passed into build_query_url() to
        # get the URL of the HTTP request. The same parameters were
        # also passed into scrubbed_url(), to get the URL that should
        # be used when storing the Representation in the database.
        params1 = novelist.build_query_url_called_with
        params2 = novelist.scrubbed_url_called_with
        eq_(params1, params2)

        eq_(
            dict(profile=novelist.profile,
                 ClientIdentifier=identifier.urn,
                 ISBN=identifier.identifier,
                 password=novelist.password,
                 version=novelist.version,
            ),
            params1
        )

        # The HTTP request went out to the query URL -- not the scrubbed URL.
        eq_(["http://query-url/"], h.requests)

        # The HTTP response was passed into novelist.review_response()
        eq_(
            (200, {'content-type': 'text/html'}, 'yay'),
            novelist.review_response_called_with
        )

        # Finally, the Representation was passed into
        # lookup_info_to_metadata, which returned a hard-coded string
        # as the final result.
        eq_("some metadata", result)

        # Looking at the Representation we can see that it was stored
        # in the database under its scrubbed URL, not the URL used to
        # make the request.
        rep = novelist.lookup_info_to_metadata_called_with
        eq_("http://scrubbed-url/", rep.url)
        eq_("yay", rep.content)

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
        # There are no books in the current library.
        eq_(items, [])

        # Set up a book for this library.
        edition = self._edition(identifier_type=Identifier.ISBN, publicationDate="2012-01-01")
        pool = self._licensepool(edition, collection=self._default_collection)
        contributor = self._contributor(sort_name=edition.sort_author, name=edition.author)

        items = self.novelist.get_items_from_query(self._default_library)

        item = dict(
            author=contributor[0]._sort_name,
            title=edition.title,
            mediaType=self.novelist.medium_to_book_format_type_values.get(edition.medium, ""),
            isbn=edition.primary_identifier.identifier,
            distributor=edition.data_source.name,
            publicationDate=edition.published.strftime("%Y%m%d")
        )

        eq_(items, [item])

    def test_create_item_object(self):
        # We pass no identifier or item to process so we get nothing back.
        (currentIdentifier, existingItem, newItem, addItem) = self.novelist.create_item_object(None, None, None)
        eq_(currentIdentifier, None)
        eq_(existingItem, None)
        eq_(newItem, None)
        eq_(addItem, False)

        # Item row from the db query
        # (identifier, identifier type, identifier,
        # edition title, edition medium, edition published date,
        # contribution role, contributor sort name
        # distributor)
        book1_from_query = (
            "12345", "Axis 360 ID", "23456",
            "Title 1", "Book", datetime.date(2002, 1, 1),
            "Author", "Author 1",
            "Gutenberg")
        book1_from_query_primary_author = (
            "12345", "Axis 360 ID", "23456",
            "Title 1", "Book", datetime.date(2002, 1, 1),
            "Primary Author", "Author 2",
            "Gutenberg")
        book1_narrator_from_query = (
            "12345", "Axis 360 ID", "23456",
            "Title 1", "Book", datetime.date(2002, 1, 1),
            "Narrator", "Narrator 1",
            "Gutenberg")
        book2_from_query = (
            "34567", "Axis 360 ID", "56789",
            "Title 2", "Book", datetime.date(1414, 1, 1),
            "Author", "Author 3",
            "Gutenberg")

        (currentIdentifier, existingItem, newItem, addItem) = (
            # params: new item, identifier, existing item
            self.novelist.create_item_object(book1_from_query, None, None)
        )
        eq_(currentIdentifier, book1_from_query[2])
        eq_(existingItem, None)
        eq_(
            newItem,
            {"isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "role": "Author",
            "author": "Author 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101"
            })
        # We want to still process this item along with the next one in case
        # the following one has the same ISBN.
        eq_(addItem, False)

        # Note that `newItem` is what we get from the previous call from `create_item_object`.
        # We are now processing the previous object along with the new one.
        # This is to check and update the value for `author` if the role changes
        # to `Primary Author`.
        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(
                book1_from_query_primary_author,
                currentIdentifier,
                newItem
            )
        )
        eq_(currentIdentifier, book1_from_query[2])
        eq_(existingItem,
            {"isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            "role": "Primary Author",
            "distributor": "Gutenberg",
            "publicationDate": "20020101"
            })
        eq_(newItem, None)
        eq_(addItem, False)

        # Test that a narrator gets added along with an author.
        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(book1_narrator_from_query, currentIdentifier, existingItem)
        )
        eq_(currentIdentifier, book1_narrator_from_query[2])
        eq_(existingItem,
            {"isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            # The role has been updated to author since the last processed item
            # has an author role. This property is eventually removed before
            # sending to Novelist so it's not really important.
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101"
            })
        eq_(newItem, None)
        eq_(addItem, False)

        # New Object
        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(book2_from_query, currentIdentifier, existingItem)
        )
        eq_(currentIdentifier, book2_from_query[2])
        eq_(existingItem,
            {"isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "author": "Author 2",
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101"
            })
        eq_(newItem,
            {"isbn": "56789",
            "mediaType": "EBook",
            "title": "Title 2",
            "role": "Author",
            "author": "Author 3",
            "distributor": "Gutenberg",
            "publicationDate": "14140101"
            })
        eq_(addItem, True)

        # New Object
        # Test that a narrator got added but not an author
        (currentIdentifier, existingItem, newItem, addItem) = (
            self.novelist.create_item_object(book1_narrator_from_query, None, None)
        )

        eq_(currentIdentifier, book1_narrator_from_query[2])
        eq_(existingItem, None)
        eq_(newItem,
            {"isbn": "23456",
            "mediaType": "EBook",
            "title": "Title 1",
            "role": "Narrator",
            "narrator": "Narrator 1",
            "distributor": "Gutenberg",
            "publicationDate": "20020101"
            })
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
            "customer": "library:yep",
            "records": []
        })

        data = [
            {"isbn":"12345", "mediaType": "http://schema.org/EBook", "title": "Book 1", "author": "Author 1" },
            {"isbn":"12346", "mediaType": "http://schema.org/EBook", "title": "Book 2", "author": "Author 2" },
        ]
        result = self.novelist.make_novelist_data_object(data)

        eq_(result, {
            "customer": "library:yep",
            "records": data
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
