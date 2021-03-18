# encoding: utf-8
# Measurement


from . import Base
from constants import DataSourceConstants

import bisect
import logging
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Unicode,
)

class Measurement(Base):
    """A  measurement of some numeric quantity associated with a
    Identifier.
    """
    __tablename__ = 'measurements'

    # Some common measurement types
    POPULARITY = u"http://librarysimplified.org/terms/rel/popularity"
    QUALITY = u"http://librarysimplified.org/terms/rel/quality"
    PUBLISHED_EDITIONS = u"http://librarysimplified.org/terms/rel/editions"
    HOLDINGS = u"http://librarysimplified.org/terms/rel/holdings"
    RATING = u"http://schema.org/ratingValue"
    DOWNLOADS = u"https://schema.org/UserDownloads"
    PAGE_COUNT = u"https://schema.org/numberOfPages"
    AWARDS = u"http://librarysimplified.org/terms/rel/awards"

    GUTENBERG_FAVORITE = u"http://librarysimplified.org/terms/rel/lists/gutenberg-favorite"

    # We have a number of ways of measuring popularity: by an opaque
    # number such as Amazon's Sales Rank, or by a directly measured
    # quantity such as the number of downloads or published editions,
    # or the number of libraries with a given book in their
    # collection.
    #
    # All of these ways of measuring popularity need to be scaled to a
    # range between 0 and 1. This nested dictionary contains
    # empirically determined measurements keyed by what they are measureing
    #
    # Each list has 100 elements. If a popularity measurement is found
    # between index n and index n+1 on a given list, it is in the nth
    # percentile and its scaled value should be n * 0.01.
    #
    # If you graphed one of these lists as a histogram you'd see how
    # values for the measured quantity are distributed, and what values
    # are above or below average.
    PERCENTILE_SCALES = {
        # A book may have a popularity score derived from an opaque
        # measure of 'popularity' from some other source.
        #
        POPULARITY : {
            # Overdrive provides a 'popularity' score for each book.
            DataSourceConstants.OVERDRIVE : [1, 1, 1, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 9, 9, 10, 10, 11, 12, 13, 14, 15, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 28, 30, 31, 33, 35, 37, 39, 41, 43, 46, 48, 51, 53, 56, 59, 63, 66, 70, 74, 78, 82, 87, 92, 97, 102, 108, 115, 121, 128, 135, 142, 150, 159, 168, 179, 190, 202, 216, 230, 245, 260, 277, 297, 319, 346, 372, 402, 436, 478, 521, 575, 632, 702, 777, 861, 965, 1100, 1248, 1428, 1665, 2020, 2560, 3535, 5805],

            # Amazon Sales Rank - lower means more sales.
            DataSourceConstants.AMAZON : [14937330, 1974074, 1702163, 1553600, 1432635, 1327323, 1251089, 1184878, 1131998, 1075720, 1024272, 978514, 937726, 898606, 868506, 837523, 799879, 770211, 743194, 718052, 693932, 668030, 647121, 627642, 609399, 591843, 575970, 559942, 540713, 524397, 511183, 497576, 483884, 470850, 458438, 444475, 432528, 420088, 408785, 398420, 387895, 377244, 366837, 355406, 344288, 333747, 324280, 315002, 305918, 296420, 288522, 279185, 270824, 262801, 253865, 246224, 238239, 230537, 222611, 215989, 208641, 202597, 195817, 188939, 181095, 173967, 166058, 160032, 153526, 146706, 139981, 133348, 126689, 119201, 112447, 106795, 101250, 96534, 91052, 85837, 80619, 75292, 69957, 65075, 59901, 55616, 51624, 47598, 43645, 39403, 35645, 31795, 27990, 24496, 20780, 17740, 14102, 10498, 7090, 3861],

            # This is as measured by the criteria defined in
            # ContentCafeSOAPClient.estimate_popularity(), in which
            # popularity is the maximum of a) the largest number of books
            # ordered in a single month within the last year, or b)
            # one-half the largest number of books ever ordered in a
            # single month.
            DataSourceConstants.CONTENT_CAFE : [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 7, 8, 9, 10, 11, 14, 18, 25, 41, 125, 387],
        },

        # The popularity of a book may be deduced from the number of
        # libraries with that book in their collections.
        #
        HOLDINGS : {
            DataSourceConstants.OCLC : [1, 8, 12, 16, 20, 24, 28, 33, 37, 43, 49, 55, 62, 70, 78, 86, 94, 102, 110, 118, 126, 134, 143, 151, 160, 170, 178, 187, 196, 205, 214, 225, 233, 243, 253, 263, 275, 286, 298, 310, 321, 333, 345, 358, 370, 385, 398, 413, 427, 443, 458, 475, 492, 511, 530, 549, 567, 586, 606, 627, 647, 669, 693, 718, 741, 766, 794, 824, 852, 882, 914, 947, 980, 1018, 1056, 1098, 1142, 1188, 1235, 1288, 1347, 1410, 1477, 1545, 1625, 1714, 1812, 1923, 2039, 2164, 2304, 2479, 2671, 2925, 3220, 3565, 3949, 4476, 5230, 7125, 34811],
        },

        # The popularity of a book may be deduced from the number of
        # published editions of that book.
        #
        PUBLISHED_EDITIONS : {
            DataSourceConstants.OCLC : [1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 14, 14, 14, 15, 15, 16, 16, 17, 18, 19, 19, 20, 21, 22, 24, 25, 26, 28, 30, 32, 34, 36, 39, 42, 46, 50, 56, 64, 73, 87, 112, 156, 281, 2812],
        },

        # The popularity of a book may be deduced from the number of
        # recent downloads from some site.
        #
        DOWNLOADS : {
            DataSourceConstants.GUTENBERG : [0, 1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 12, 12, 12, 13, 14, 14, 15, 15, 16, 16, 17, 18, 18, 19, 19, 20, 21, 21, 22, 23, 23, 24, 25, 26, 27, 28, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 40, 41, 43, 45, 46, 48, 50, 52, 55, 57, 60, 62, 65, 69, 72, 76, 79, 83, 87, 93, 99, 106, 114, 122, 130, 140, 152, 163, 179, 197, 220, 251, 281, 317, 367, 432, 501, 597, 658, 718, 801, 939, 1065, 1286, 1668, 2291, 4139],
        },
    }

    # Ratings are issued on a scale which differs from one data source
    # to another. Once we know the scale used by a given data source, we can
    # scale its ratings to the 0..1 range and create a 'quality' rating.
    RATING_SCALES = {
        DataSourceConstants.OVERDRIVE : [1, 5],
        DataSourceConstants.AMAZON : [1, 5],
        DataSourceConstants.UNGLUE_IT: [1, 5],
        DataSourceConstants.NOVELIST: [0, 5],
        DataSourceConstants.LIBRARY_STAFF: [1, 5],
    }

    id = Column(Integer, primary_key=True)

    # A Measurement is always associated with some Identifier.
    identifier_id = Column(
        Integer, ForeignKey('identifiers.id'), index=True)

    # A Measurement always comes from some DataSource.
    data_source_id = Column(
        Integer, ForeignKey('datasources.id'), index=True)

    # The quantity being measured.
    quantity_measured = Column(Unicode, index=True)

    # The measurement itself.
    value = Column(Float)

    # The measurement normalized to a 0...1 scale.
    _normalized_value = Column(Float, name="normalized_value")

    # How much weight should be assigned this measurement, relative to
    # other measurements of the same quantity from the same source.
    weight = Column(Float, default=1)

    # When the measurement was taken
    taken_at = Column(DateTime, index=True)

    # True if this is the most recent measurement of this quantity for
    # this Identifier.
    #
    is_most_recent = Column(Boolean, index=True)

    def __repr__(self):
        return "%s(%r)=%s (norm=%.2f)" % (
            self.quantity_measured, self.identifier, self.value,
            self.normalized_value or 0)

    @classmethod
    def overall_quality(cls, measurements, popularity_weight=0.3,
                        rating_weight=0.7, default_value=0):
        """Turn a bunch of measurements into an overall measure of quality."""
        if popularity_weight + rating_weight != 1.0:
            raise ValueError(
                "Popularity weight and rating weight must sum to 1! (%.2f + %.2f)" % (
                    popularity_weight, rating_weight)
        )
        popularities = []
        ratings = []
        qualities = []
        for m in measurements:
            l = None
            if m.quantity_measured == cls.RATING:
                l = ratings
            elif m.quantity_measured == cls.QUALITY:
                l = qualities
            else:
                # NOTE: This is assuming that everything in PERCENTILE_SCALES
                # is an opaque measure of 'popularity'.
                l = popularities
            if l is not None:
                l.append(m)
        popularity = cls._average_normalized_value(popularities)
        rating = cls._average_normalized_value(ratings)
        quality = cls._average_normalized_value(qualities)
        if popularity is None and rating is None and quality is None:
            # We have absolutely no idea about the quality of this work.
            return default_value
        if popularity is not None and rating is None and quality is None:
            # Our idea of the quality depends entirely on the work's popularity.
            return popularity
        if rating is not None and popularity is None and quality is None:
            # Our idea of the quality depends entirely on the work's rating.
            return rating
        if quality is not None and rating is None and popularity is None:
            # Our idea of the quality depends entirely on the work's quality scores.
            return quality

        # We have at least two of the three... but which two?
        if popularity is None:
            # We have rating and quality but not popularity.
            final = rating
        elif rating is None:
            # We have quality and popularity but not rating.
            final = popularity
        else:
            # We have popularity and rating but not quality.
            final = (popularity * popularity_weight) + (rating * rating_weight)
            logging.debug(
                "(%.2f * %.2f) + (%.2f * %.2f) = %.2f",
                popularity, popularity_weight, rating, rating_weight, final
            )
        if quality:
            logging.debug("Popularity+Rating: %.2f, Quality: %.2f" % (final, quality))
            final = (final / 2) + (quality / 2)
            logging.debug("Final value: %.2f" % final)
        return final

    @classmethod
    def _average_normalized_value(cls, measurements):
        num_measurements = 0
        measurement_total = 0
        for m in measurements:
            v = m.normalized_value
            if v is None:
                continue
            num_measurements += m.weight
            measurement_total += (v * m.weight)
        if num_measurements:
            return measurement_total / num_measurements
        else:
            return None

    @property
    def normalized_value(self):
        """Normalize a measured value, possibly using the rating scales in
        RATING_SCALES or the empirically determined percentile scales
        in PERCENTILE_SCALES.
        """
        if self._normalized_value:
            pass
        elif self.value is None:
            return None
        elif self.data_source.name == DataSourceConstants.METADATA_WRANGLER:
            # Data from the metadata wrangler comes in pre-normalized.
            self._normalized_value = self.value
        elif (self.quantity_measured == self.RATING
              and self.data_source.name in self.RATING_SCALES):
            # Ratings need to be normalized from a scale that depends
            # on the data source (e.g. Amazon's 1-5 stars) to a 0..1 scale.
            scale_min, scale_max = self.RATING_SCALES[self.data_source.name]
            width = float(scale_max-scale_min)
            value = self.value-scale_min
            self._normalized_value = value / width
        elif self.quantity_measured in self.PERCENTILE_SCALES:
            # Other measured quantities need to be normalized using
            # a percentile scale determined emperically.
            by_data_source = self.PERCENTILE_SCALES[
                self.quantity_measured
            ]
            if not self.data_source.name in by_data_source:
                # We don't know how to normalize measurements from
                # this data source. Ignore this data.
                return None
            percentiles = by_data_source[self.data_source.name]
            position = bisect.bisect_left(percentiles, self.value)
            self._normalized_value = position * 0.01

        return self._normalized_value
