import pytest

from ...model import (
    DataSource,
    Measurement,
    get_one_or_create
)

from ...util.datetime_helpers import datetime_utc

class TestMeasurement:

    @pytest.fixture(autouse=True)
    def setup_method(self, db_session):
        self._db = db_session
        self.SOURCE_NAME = "Test Data Source"

        # Create a test DataSource
        obj, _ = get_one_or_create(
                self._db, DataSource,
                name=self.SOURCE_NAME,
        )
        self.source = obj

        Measurement.PERCENTILE_SCALES[Measurement.POPULARITY][self.SOURCE_NAME] = [
            1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 9, 10, 10, 11, 12, 13, 14, 15, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 28, 30, 31, 33, 35, 37, 39, 41, 43, 46, 48, 51, 53, 56, 59, 63, 66, 70, 74, 78, 82, 87, 92, 97, 102, 108, 115, 121, 128, 135, 142, 150, 159, 168, 179, 190, 202, 216, 230, 245, 260, 277, 297, 319, 346, 372, 402, 436, 478, 521, 575, 632, 702, 777, 861, 965, 1100, 1248, 1428, 1665, 2020, 2560, 3535, 5805]
        Measurement.RATING_SCALES[self.SOURCE_NAME] = [1, 10]

    def _measurement(self, quantity, value, source, weight):
        source = source or self.source
        return Measurement(
            data_source=source, quantity_measured=quantity,
            value=value, weight=weight)

    def _popularity(self, value, source=None, weight=1):
        return self._measurement(Measurement.POPULARITY, value, source, weight)

    def _rating(self, value, source=None, weight=1):
        return self._measurement(Measurement.RATING, value, source, weight)

    def _quality(self, value, weight=1):
        # The only source we recognize for quality scores is the metadata
        # wrangler.
        source = DataSource.lookup(self._db, DataSource.METADATA_WRANGLER)
        return self._measurement(Measurement.QUALITY, value, source, weight)

    def test_newer_measurement_displaces_earlier_measurement(self, create_identifier):
        """
        GIVEN: A Measurement on an Identifier
        WHEN:  A new Measurement is added to an Identifier
        THEN:  The newer Measurement is the most recent
        """
        wi = create_identifier(self._db)
        m1 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 10)
        assert True == m1.is_most_recent

        m2 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 11)
        assert False == m1.is_most_recent
        assert True == m2.is_most_recent

        m3 = wi.add_measurement(self.source, Measurement.POPULARITY, 11)
        assert True == m2.is_most_recent
        assert True == m3.is_most_recent


    def test_can_insert_measurement_after_the_fact(self, create_identifier):
        """
        GIVEN: A Measurement on an Identifier
        WHEN:  Adding a Measurement that is older than the first Measurement
        THEN:  The first Measurement is the most recent
        """

        old = datetime_utc(2011, 1, 1)
        new = datetime_utc(2012, 1, 1)

        wi = create_identifier(self._db)
        m1 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 10,
                                taken_at=new)
        assert True == m1.is_most_recent

        m2 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 5,
                                taken_at=old)
        assert True == m1.is_most_recent

    def test_normalized_popularity(self):
        """
        GIVEN: A popularity number
        WHEN:  Normalizing the popularity numer
        THEN:  The popularity number is normalized according to PERCENTILE_SCALES
        """
        # Here's a very popular book on the scale defined in
        # PERCENTILE_SCALES[POPULARITY].
        p = self._popularity(6000)
        assert 1.0 == p.normalized_value

        # Here's a slightly less popular book.
        p = self._popularity(5804)
        assert 0.99 == p.normalized_value

        # Here's a very unpopular book
        p = self._popularity(1)
        assert 0 == p.normalized_value

        # Here's a book in the middle.
        p = self._popularity(59)
        assert 0.5 == p.normalized_value

        # So long as the data source and the quantity measured can be
        # found in PERCENTILE_SCALES, the data can be normalized.

        # This book is extremely unpopular.
        overdrive = DataSource.lookup(self._db, DataSource.OVERDRIVE)
        m = self._measurement(Measurement.POPULARITY, 0, overdrive, 10)
        assert 0 == m.normalized_value

        # For some other data source, we don't know whether popularity=0
        # means 'very popular' or 'very unpopular'.
        gutenberg = DataSource.lookup(self._db, DataSource.GUTENBERG)
        m = self._measurement(Measurement.POPULARITY, 0, gutenberg, 10)
        assert None == m.normalized_value

        # We also don't know what it means if Overdrive were to say
        # that a book got 200 downloads. Is that a lot? Compared to
        # what? In what time period? We would have to measure it to
        # find out -- at that point we would put the percentile list
        # in PERCENTILE_SCALES and this would start working.
        m = self._measurement(Measurement.DOWNLOADS, 0, overdrive, 10)
        assert None == m.normalized_value

    def test_normalized_rating(self):
        """
        GIVEN: A rating number
        WHEN:  Normalizing the rating number
        THEN:  The rating number is normalized according to RATING_SCALES
        """
        # Here's a very good book on the scale defined in
        # RATING_SCALES.
        p = self._rating(10)
        assert 1.0 == p.normalized_value

        # Here's a slightly less good book.
        p = self._rating(9)
        assert 8.0/9 == p.normalized_value

        # Here's a very bad book
        p = self._rating(1)
        assert 0 == p.normalized_value

    def test_neglected_source_cannot_be_normalized(self):
        """
        GIVEN: A DataSource that has no normalization scales
        WHEN:  Normalizing a number
        THEN:  None is returned
        """
        obj, _ = get_one_or_create(
                self._db, DataSource,
                name="Neglected source"
        )
        neglected_source = obj
        p = self._popularity(100, neglected_source)
        assert None == p.normalized_value

        r = self._rating(100, neglected_source)
        assert None == r.normalized_value

    def test_overall_quality(self):
        """
        GIVEN: A popularity, rating, and irrelevant measurements
        WHEN:  Measuring overall quality
        THEN:  Measurement quality is correctly calculated
        """
        popularity = self._popularity(59)
        rating = self._rating(4)
        irrelevant = self._measurement("Some other quantity", 42, self.source, 1)
        pop = popularity.normalized_value
        rat = rating.normalized_value
        assert 0.5 == pop
        assert 1.0/3 == rat
        l = [popularity, rating, irrelevant]
        quality = Measurement.overall_quality(l)
        assert (0.7*rat)+(0.3*pop) == quality

        # Mess with the weights.
        assert (0.5*rat)+(0.5*pop) == Measurement.overall_quality(l, 0.5, 0.5)

        # Adding a non-popularity measurement that is _equated_ to
        # popularity via a percentile scale modifies the
        # normalized value -- we don't care exactly how, only that
        # it's taken into account.
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        popularityish = self._measurement(
            Measurement.HOLDINGS, 400, oclc, 10
        )
        new_quality = Measurement.overall_quality(l + [popularityish])
        assert quality != new_quality

    def test_overall_quality_based_solely_on_popularity_if_no_rating(self):
        """
        GIVEN: Only a popularity number
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is correctly calculated
        """
        pop = self._popularity(59)
        assert 0.5 == Measurement.overall_quality([pop])

    def test_overall_quality_with_rating_and_quality_but_not_popularity(self):
        """
        GIVEN: A Measurement rating and quality score
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is correctly calculated
        """
        rat = self._rating(4)
        qual = self._quality(0.5)

        # We would expect the final quality score to be 1/2 of the quality
        # score we got from the metadata wrangler, and 1/2 of the normalized
        # value of the 4-star rating.
        expect = (rat.normalized_value / 2) + 0.25
        assert expect == Measurement.overall_quality([rat, qual], 0.5, 0.5)

    def test_overall_quality_with_popularity_and_quality_but_not_rating(self):
        """
        GIVEN: A Measurement popularity and quality score
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is correctly calculated
        """
        pop = self._popularity(4)
        qual = self._quality(0.5)

        # We would expect the final quality score to be 1/2 of the quality
        # score we got from the metadata wrangler, and 1/2 of the normalized
        # value of the 4-star rating.
        expect = (pop.normalized_value / 2) + (0.5/2)
        assert expect == Measurement.overall_quality([pop, qual], 0.5, 0.5)

    def test_overall_quality_with_popularity_quality_and_rating(self):
        """
        GIVEN: A Measurement with popularity, rating, and quality scor
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is correctly calculated
        """
        pop = self._popularity(4)
        rat = self._rating(4)
        quality_score = 0.66
        qual = self._quality(quality_score)

        # The popularity and rating are scaled appropriately and
        # added together.
        expect_1 = (pop.normalized_value * 0.75) + (rat.normalized_value*0.25)

        # Then the whole thing is divided in half and added to half of the
        # quality score
        expect_total = (expect_1/2 + (quality_score/2))
        assert expect_total == Measurement.overall_quality([pop, rat, qual], 0.75, 0.25)

    def test_overall_quality_takes_weights_into_account(self):
        """
        GIVEN: A Measurement with ratings that have weights
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is correctly calculated
        """
        rating1 = self._rating(10, weight=10)
        rating2 = self._rating(1, weight=1)
        assert 0.91 == round(Measurement.overall_quality([rating1, rating2]),2)

    def test_overall_quality_is_zero_if_no_relevant_measurements(self):
        """
        GIVEN: A Measurement with a number of irrelevant type
        WHEN:  Measuring the overall quality
        THEN:  Overall quality is 0
        """
        irrelevant = self._measurement("Some other quantity", 42, self.source, 1)
        assert 0 == Measurement.overall_quality([irrelevant])

    def test_calculate_quality(self, create_identifier, create_work):
        """
        GIVEN: A Work with with an Identifier that has Measurements
        WHEN:  Assessing the quality with another DataSource Identifier
        THEN:  The Work quality accurately reflects the Measurements
        """
        w = create_work(self._db, with_open_access_download=True)

        # This book used to be incredibly popular.
        identifier = w.presentation_edition.primary_identifier
        old_popularity = identifier.add_measurement(
            self.source, Measurement.POPULARITY, 6000)

        # Now it's just so-so.
        popularity = identifier.add_measurement(
            self.source, Measurement.POPULARITY, 59)

        # This measurement is irrelevant because "Test Data Source"
        # doesn't have a mapping from number of editions to a
        # percentile range.
        irrelevant = identifier.add_measurement(
            self.source, Measurement.PUBLISHED_EDITIONS, 42)

        # If we calculate the quality based solely on the primary
        # identifier, only the most recent popularity is considered,
        # and the book ends up in the middle of the road in terms of
        # quality.
        w.calculate_quality([identifier.id])
        assert 0.5 == w.quality

        old_quality = w.quality

        # But let's say there's another identifier that's equivalent,
        # and it has a number of editions that was obtained from
        # OCLC Classify, which _does_ have a mapping from number
        # of editions to a percentile range.
        wi = create_identifier(self._db)
        oclc = DataSource.lookup(self._db, DataSource.OCLC)
        wi.add_measurement(oclc, Measurement.PUBLISHED_EDITIONS, 800)

        # Now the quality is higher--the large OCLC PUBLISHED_EDITIONS
        # measurement bumped it up.
        w.calculate_quality([identifier.id, wi.id])
        assert w.quality > old_quality

    def test_calculate_quality_default_quality(self, create_work):
        """
        GIVEN: A Work
        WHEN:  Assessing the quality with a default setting
        THEN:  The Work quality is the default setting
        """

        # Here's a work with no measurements whatsoever.
        w = create_work(self._db)

        # Its quality is dependent entirely on the default value we
        # pass into calculate_quality
        w.calculate_quality([])
        assert 0 == w.quality
        w.calculate_quality([], 0.4)
        assert 0.4 == w.quality
