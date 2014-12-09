import datetime

from nose.tools import (
    assert_raises_regexp,
    eq_,
    set_trace,
)

from model import (
    DataSource,
    Measurement,
    get_one_or_create
)

from testing import (
    DatabaseTest,
)

class TestMeasurement(DatabaseTest):

    def setup(self):
        super(TestMeasurement, self).setup()
        self.SOURCE_NAME = "Test Data Source"

        # Create a test DataSource
        obj, new = get_one_or_create(
                self._db, DataSource,
                name=self.SOURCE_NAME,
        )
        self.source = obj

        Measurement.POPULARITY_PERCENTILES[self.SOURCE_NAME] = [
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

    def test_newer_measurement_displaces_earlier_measurement(self):
        wi = self._identifier()
        m1 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 10)
        eq_(True, m1.is_most_recent)

        m2 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 11)
        eq_(False, m1.is_most_recent)
        eq_(True, m2.is_most_recent)

        m3 = wi.add_measurement(self.source, Measurement.POPULARITY, 11)
        eq_(True, m2.is_most_recent)
        eq_(True, m3.is_most_recent)


    def test_can_insert_measurement_after_the_fact(self):
        
        old = datetime.datetime(2011, 1, 1)
        new = datetime.datetime(2012, 1, 1)

        wi = self._identifier()
        m1 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 10,
                                taken_at=new)
        eq_(True, m1.is_most_recent)

        m2 = wi.add_measurement(self.source, Measurement.DOWNLOADS, 5,
                                taken_at=old)
        eq_(True, m1.is_most_recent)

    def test_normalized_popularity(self):
        # Here's a very popular book on the scale defined in
        # POPULARITY_PERCENTILES.
        p = self._popularity(6000)
        eq_(1.0, p.normalized_value)

        # Here's a slightly less popular book.
        p = self._popularity(5804)
        eq_(0.99, p.normalized_value)
            
        # Here's a very unpopular book
        p = self._popularity(1)
        eq_(0, p.normalized_value)

        # Here's a book in the middle.
        p = self._popularity(59)
        eq_(0.5, p.normalized_value)

    def test_normalized_rating(self):
        # Here's a very good book on the scale defined in
        # RATING_SCALES.
        p = self._rating(10)
        eq_(1.0, p.normalized_value)

        # Here's a slightly less good book.
        p = self._rating(9)
        eq_(8.0/9, p.normalized_value) 
           
        # Here's a very bad book
        p = self._rating(1)
        eq_(0, p.normalized_value)

    def test_neglected_source_cannot_be_normalized(self):
        obj, new = get_one_or_create(
                self._db, DataSource,
                name="Neglected source"
        )
        neglected_source = obj
        p = self._popularity(100, neglected_source)
        eq_(None, p.normalized_value)

        r = self._rating(100, neglected_source)
        eq_(None, r.normalized_value)

    def test_overall_quality(self):
        popularity = self._popularity(59)
        rating = self._rating(4)
        irrelevant = self._measurement("Some other quantity", 42, self.source, 1)
        pop = popularity.normalized_value
        rat = rating.normalized_value
        eq_(0.5, pop)
        eq_(1.0/3, rat) 
        l = [popularity, rating, irrelevant]
        eq_((0.7*rat)+(0.3*pop), Measurement.overall_quality(l))

        # Mess with the weights.
        eq_((0.5*rat)+(0.5*pop), Measurement.overall_quality(l, 0.5, 0.5))

    def test_overall_quality_based_solely_on_popularity_if_no_rating(self):
        pop = self._popularity(59)
        eq_(0.5, Measurement.overall_quality([pop]))

    def test_overall_quality_takes_weights_into_account(self):
        rating1 = self._rating(10, weight=10)
        rating2 = self._rating(1, weight=1)
        eq_(0.91, round(Measurement.overall_quality([rating1, rating2]),2))

    def test_overall_quality_is_zero_if_no_relevant_measurements(self):
        irrelevant = self._measurement("Some other quantity", 42, self.source, 1)
        eq_(0, Measurement.overall_quality([irrelevant]))

    def test_calculate_quality(self):
        w = self._work()

        # This book used to be incredibly popular.
        identifier = w.primary_edition.primary_identifier
        old_popularity = identifier.add_measurement(
            self.source, Measurement.POPULARITY, 6000)

        # Now it's just so-so.
        popularity = identifier.add_measurement(
            self.source, Measurement.POPULARITY, 59)

        # This measurement is irrelevant.
        irrelevant = identifier.add_measurement(
            self.source, "Some other quantity", 42)

        # If we calculate the quality based solely on the primary
        # identifier, only the most recent popularity is considered,
        # and the book ends up in the middle of the road in terms of
        # quality.
        w.calculate_quality([identifier.id])
        eq_(0.5, w.quality)

        old_quality = w.quality

        # But let's say there's another identifier that's equivalent,
        # and it has a rating.
        wi = self._identifier()
        wi.add_measurement(self.source, Measurement.RATING, 8)

        # Now the quality is higher--the high quality measurement
        # bumped it up.
        w.calculate_quality([identifier.id, wi.id])
        assert w.quality > old_quality

