import json
from nose.tools import (
    set_trace,
    eq_,
    assert_raises,
)

from . import DatabaseTest, sample_data

from core.config import (
    Configuration,
    temp_config,
)
from core.metadata_layer import Metadata
from core.model import (
    get_one,
    get_one_or_create,
    DataSource,
    Edition,
    Identifier,
    Representation,
)
from api.novelist import (
    MockNoveListAPI,
    NoveListAPI,
    NoveListCoverageProvider,
)


class TestNoveListAPI(DatabaseTest):
    """Tests the NoveList API service object"""

    def setup(self):
        super(TestNoveListAPI, self).setup()
        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            self.novelist = NoveListAPI.from_config(self._db)

    def sample_data(self, filename):
        return sample_data(filename, 'novelist')

    def sample_representation(self, filename):
        content = self.sample_data(filename)
        return self._representation(
            media_type='application/json', content=content
        )[0]

    def test_from_config(self):
        """Confirms that NoveListAPI can be built from config successfully"""

        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            novelist = NoveListAPI.from_config(self._db)
            eq_(True, isinstance(novelist, NoveListAPI))
            eq_("library", novelist.profile)
            eq_("yep", novelist.password)

            # Without either configuration value, an error is raised.
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library"
            }
            assert_raises(ValueError, NoveListAPI.from_config, self._db)
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            assert_raises(ValueError, NoveListAPI.from_config, self._db)

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

    def test_choose_best_metadata(self):
        more_identifier = self._identifier(identifier_type=Identifier.NOVELIST_ID)
        less_identifier = self._identifier(identifier_type=Identifier.NOVELIST_ID)
        metadatas = [Metadata(DataSource.NOVELIST, primary_identifier=more_identifier)]

        # When only one Metadata object is given, that object is returned.
        result = self.novelist.choose_best_metadata(metadatas, self._identifier())
        eq_(True, isinstance(result, Metadata))
        eq_(metadatas[0], self.novelist.choose_best_metadata(metadatas, self._identifier()))

        # When top identifiers have equal representation, the method returns none.
        metadatas.append(Metadata(DataSource.NOVELIST, primary_identifier=less_identifier))
        eq_(None, self.novelist.choose_best_metadata(metadatas, self._identifier()))

        # But when one pulls ahead, we get the metadata object again.
        metadatas.append(Metadata(DataSource.NOVELIST, primary_identifier=more_identifier))
        result = self.novelist.choose_best_metadata(metadatas, self._identifier())
        eq_(True, isinstance(result, tuple))
        metadata, confidence = result
        eq_(True, isinstance(metadata, Metadata))
        eq_(0.67, round(confidence, 2))
        eq_(more_identifier, metadata.primary_identifier)


class TestNoveListCoverageProvider(DatabaseTest):

    def setup(self):
        super(TestNoveListCoverageProvider, self).setup()
        with temp_config() as config:
            config['integrations'][Configuration.NOVELIST_INTEGRATION] = {
                Configuration.NOVELIST_PROFILE : "library",
                Configuration.NOVELIST_PASSWORD : "yep"
            }
            self.novelist = NoveListCoverageProvider(self._db)
        self.novelist.api = MockNoveListAPI()

        self.metadata = Metadata(
            data_source = self.novelist.source,
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
            self._db, Edition, data_source=self.novelist.source,
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
            self._db, Edition, data_source=self.novelist.source,
            primary_identifier=identifier
        )
        assert novelist_edition
        eq_(self.metadata.series, novelist_edition.series)
        eq_(self.metadata.series_position, novelist_edition.series_position)
        # Other basic metadata is also stored.
        eq_(self.metadata.title, novelist_edition.title)
