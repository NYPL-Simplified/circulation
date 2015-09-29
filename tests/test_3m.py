from nose.tools import set_trace, eq_
import datetime
import os
from model import (
    Contributor,
    Resource,
    Hyperlink,
    Identifier,
    Edition,
    Subject,
    Measurement,
)
from threem import (
    ItemListParser,
)

class TestItemListParser(object):

    base_path = os.path.split(__file__)[0]
    resource_path = os.path.join(base_path, "files", "3m")

    @classmethod
    def get_data(cls, filename):
        path = os.path.join(cls.resource_path, filename)
        return open(path).read()

    def test_parse_author_string(cls):
        authors = ItemListParser.author_names_from_string(
            "Walsh, Jill Paton; Sayers, Dorothy L.")
        eq_([x.sort_name for x in authors], 
            ["Walsh, Jill Paton", "Sayers, Dorothy L."]
        )
        eq_([x.role for x in authors],
            [Contributor.AUTHOR_ROLE, Contributor.AUTHOR_ROLE]
        )

        [author] = ItemListParser.author_names_from_string(
            "Baum, Frank L. (Frank Lyell)")
        eq_("Baum, Frank L.", author.sort_name)

    def test_parse_genre_string(self):
        def f(genre_string):
            genres = ItemListParser.parse_genre_string(genre_string)
            assert all([x.type == Subject.THREEM for x in genres])
            return [x.identifier for x in genres]

        eq_(["Children's Health", "Health"], 
            f("Children&amp;#39;s Health,Health,"))
        
        eq_(["Action & Adventure", "Science Fiction", "Fantasy", "Magic",
             "Renaissance"], 
            f("Action &amp;amp; Adventure,Science Fiction, Fantasy, Magic,Renaissance,"))

    def test_item_list(cls):
        data = cls.get_data("item_metadata_list.xml")
        
        data = [(id, raw, cooked)
                for (id, raw,cooked)  in ItemListParser().parse(data)]

        # There should be 25 items in the list.
        eq_(25, len(data))

        set_trace()

        # Do a spot check of the first item in the list
        id, raw, cooked = data[0]

        eq_("ddf4gr9", id)

        assert raw.startswith("<Item")

        eq_("The Incense Game", cooked.title)
        eq_("A Novel of Feudal Japan", cooked.subtitle)
        eq_("eng", cooked.language)
        eq_("St. Martin's Press", cooked.publisher)
        eq_(datetime.datetime(year=2012, month=9, day=17), 
            cooked.published
        )

        primary = cooked.primary_identifier
        eq_("ddf4gr9", primary.identifier)
        eq_(Identifier.THREEM_ID, primary.type)

        [isbn] = cooked.identifiers
        eq_("9781250015280", isbn.identifier)
        eq_(Identifier.ISBN, isbn.type)

        [author] = cooked.contributors
        eq_("Rowland, Laura Joh", author.sort_name)
        eq_(Contributor.AUTHOR_ROLE, author.role)

        subjects = [x.identifier for x in cooked.subjects]
        eq_(["Children's Health", "Mystery & Detective"], sorted(subjects))

        [pages] = cooked.measurements
        eq_(Measurement.PAGE_COUNT, pages.quantity_measured)
        eq_(304, pages.value)

        [alternate, image, description] = sorted(
            cooked.links, key = lambda x: x.rel)
        set_trace()
        eq_("alternate", alternate.rel)
        assert alternate.href.startswith("http://ebook.3m.com/library")

        eq_(Hyperlink.IMAGE_REL, image.rel)
        assert image.href.startswith("http://ebook.3m.com/delivery")

        eq_(Hyperlink.DESCRIPTION_REL, description.rel)
        assert description.value.startswith("<b>Winner")
