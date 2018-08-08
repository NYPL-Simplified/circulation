from model import (
    MaterializedWork,
    MaterializedWorkWithGenre,
)

from feed import WorkFeed

class MaterializedWorkLaneFeed(WorkFeed):

    """A WorkFeed where all the works come from a predefined lane."""

    active_facet_for_field = {
        MaterializedWork.sort_title : "title",
        MaterializedWork.sort_author : "author",
    }
    order_facet_to_database_field = {
        "title" : MaterializedWork.sort_title,
        "author" : MaterializedWork.sort_author,
    }
    default_sort_order = [
        MaterializedWork.sort_title, MaterializedWork.sort_author,
        MaterializedWork.license_pool_id
    ]

    license_pool_field = MaterializedWork.license_pool

    @classmethod
    def factory(self, lane, *args, **kwargs):
        if lane.genres:
            feed = MaterializedWorkWithGenreLaneFeed(
                lane, *args, **kwargs)
        else:
            feed = MaterializedWorkLaneFeed(lane, *args, **kwargs)
        return feed

    def __init__(self, lane, *args, **kwargs):
        self.lane = lane
        super(MaterializedWorkLaneFeed, self).__init__(*args, **kwargs)

    def base_query(self, _db):
        return self.lane.materialized_works(self.languages)


class MaterializedWorkWithGenreLaneFeed(MaterializedWorkLaneFeed):

    """A WorkFeed where all the works come from a predefined lane."""

    active_facet_for_field = {
        MaterializedWorkWithGenre.sort_title : "title",
        MaterializedWorkWithGenre.sort_author : "author",
    }
    order_facet_to_database_field = {
        "title" : MaterializedWorkWithGenre.sort_title,
        "author" : MaterializedWorkWithGenre.sort_author,
    }
    default_sort_order = [
        MaterializedWorkWithGenre.sort_title,
        MaterializedWorkWithGenre.sort_author,
        MaterializedWorkWithGenre.license_pool_id,
    ]

    license_pool_field = MaterializedWorkWithGenre.license_pool
