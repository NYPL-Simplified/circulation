from nose.tools import (
    eq_,
    set_trace,
)

from api.config import (
    Configuration,
    temp_config,
    FacetConfig
)
from core.facets import FacetConstants as Facets

class TestFacetConfig(object):

    def setup(self):
        self.enabled = {
            Facets.ORDER_FACET_GROUP_NAME: [
                Facets.ORDER_TITLE, Facets.ORDER_AUTHOR
            ]
        }

        self.default = {
            Facets.ORDER_FACET_GROUP_NAME: Facets.ORDER_TITLE
        }

    def test_from_config(self):
        with temp_config() as config:
            config[Configuration.POLICIES] = {}

            facet_config = FacetConfig.from_config()
            eq_(Configuration.DEFAULT_ENABLED_FACETS, facet_config._enabled_facets)
            eq_(Configuration.DEFAULT_FACET, facet_config._default_facets)

        with temp_config() as config:
            config[Configuration.POLICIES][Configuration.FACET_POLICY] = {
                Configuration.ENABLED_FACETS_KEY: self.enabled,
                Configuration.DEFAULT_FACET_KEY: self.default,
            }

            facet_config = FacetConfig.from_config()
            eq_(self.enabled, facet_config._enabled_facets)
            eq_(self.default, facet_config._default_facets)

    def test_enabled_facets(self):
        facet_config = FacetConfig(self.enabled, self.default)

        eq_([Facets.ORDER_TITLE, Facets.ORDER_AUTHOR], facet_config.enabled_facets(Facets.ORDER_FACET_GROUP_NAME))
        eq_(None, facet_config.enabled_facets(Facets.COLLECTION_FACET_GROUP_NAME))

    def test_default_facet(self):
        facet_config = FacetConfig(self.enabled, self.default)

        eq_(Facets.ORDER_TITLE, facet_config.default_facet(Facets.ORDER_FACET_GROUP_NAME))
        eq_(None, facet_config.default_facet(Facets.COLLECTION_FACET_GROUP_NAME))

    def test_enable_facet(self):
        facet_config = FacetConfig(self.enabled, self.default)

        facet_config.enable_facet(Facets.ORDER_FACET_GROUP_NAME, Facets.ORDER_SERIES_POSITION)
        eq_([Facets.ORDER_TITLE, Facets.ORDER_AUTHOR, Facets.ORDER_SERIES_POSITION],
            facet_config.enabled_facets(Facets.ORDER_FACET_GROUP_NAME))

        facet_config.enable_facet(Facets.COLLECTION_FACET_GROUP_NAME, Facets.COLLECTION_MAIN)
        eq_([Facets.COLLECTION_MAIN], facet_config.enabled_facets(Facets.COLLECTION_FACET_GROUP_NAME))

    def test_set_default_facet(self):
        facet_config = FacetConfig(self.enabled, self.default)

        facet_config.set_default_facet(Facets.ORDER_FACET_GROUP_NAME, Facets.ORDER_AUTHOR)
        eq_(Facets.ORDER_AUTHOR, facet_config.default_facet(Facets.ORDER_FACET_GROUP_NAME))
        
        facet_config.set_default_facet(Facets.ORDER_FACET_GROUP_NAME, Facets.ORDER_SERIES_POSITION)
        eq_(Facets.ORDER_SERIES_POSITION, facet_config.default_facet(Facets.ORDER_FACET_GROUP_NAME))
        eq_([Facets.ORDER_TITLE, Facets.ORDER_AUTHOR, Facets.ORDER_SERIES_POSITION],
            facet_config.enabled_facets(Facets.ORDER_FACET_GROUP_NAME))

        facet_config.set_default_facet(Facets.COLLECTION_FACET_GROUP_NAME, Facets.COLLECTION_MAIN)
        eq_(Facets.COLLECTION_MAIN, facet_config.default_facet(Facets.COLLECTION_FACET_GROUP_NAME))
        eq_([Facets.COLLECTION_MAIN], facet_config.enabled_facets(Facets.COLLECTION_FACET_GROUP_NAME))
