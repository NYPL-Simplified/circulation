from model import (
    MaterializedWork,
    MaterializedWorkWithGenre,
    WorkFeed,
)

class MaterializedWorkLaneFeed(WorkFeed):

    """A WorkFeed where all the works come from a predefined lane."""

    active_facet_for_field = {
        MaterializedWork.sort_title : "title",
        MaterializedWork.sort_author : "author",
    }
    field_for_active_facet = dict(
        (v,k) for k,v in active_facet_for_field.items())
    default_sort_order = [
        MaterializedWork.sort_title, MaterializedWork.sort_author,
        MaterializedWork.works_id]

    license_pool_field = MaterializedWork.license_pool

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
    field_for_active_facet = dict(
        (v,k) for k,v in active_facet_for_field.items())
    default_sort_order = [
        MaterializedWorkWithGenre.sort_title,
        MaterializedWorkWithGenre.sort_author,
        MaterializedWorkWithGenre.works_id]

    license_pool_field = MaterializedWorkWithGenre.license_pool
