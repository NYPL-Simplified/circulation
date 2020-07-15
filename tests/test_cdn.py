# encoding: utf-8
from nose.tools import (
    eq_,
    set_trace,
)
from . import DatabaseTest

from ..config import Configuration, temp_config
from ..model import ExternalIntegration
from ..cdn import cdnify


class TestCDN(DatabaseTest):

    def unchanged(self, url, cdns):
        self.ceq(url, url, cdns)

    def ceq(self, expect, url, cdns):
        cdns = cdns or {}
        with temp_config() as config:
            config[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = cdns
            config[Configuration.CDNS_LOADED_FROM_DATABASE] = True
            eq_(expect, cdnify(url))

    def test_no_cdns(self):
        url = "http://foo/"
        self.unchanged(url, {})

    def test_non_matching_cdn(self):
        url = "http://foo.com/bar"
        self.unchanged(url, {"bar.com" : "cdn.com"})

    def test_matching_cdn(self):
        url = "http://foo.com/bar#baz"
        self.ceq("https://cdn.org/bar#baz", url,
                 {"foo.com" : "https://cdn.org",
                  "bar.com" : "http://cdn2.net/"}
        )

    def test_relative_url(self):
        # By default, relative URLs are untouched.
        url = "/groups/"
        self.unchanged(url, {"bar.com" : "cdn.com"})

        # But if the CDN list has an entry for the empty string, that
        # URL is used for relative URLs.
        self.ceq("https://cdn.org/groups/", url,
                 {"" : "https://cdn.org/"})
