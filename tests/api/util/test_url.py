import pytest

from api.util.url import URLUtility


class TestURLUtility:
    @pytest.mark.parametrize(
        "candidate,url_list,result",
        [
            pytest.param(
                "http://librarysimplified.org",
                ["http://librarysimplified.org"],
                True,
                id="simplest_full_match"
            ),
            pytest.param(
                "capacitor://localhost",
                ["capacitor://localhost"],
                True,
                id="simplest_full_match"
            ),
            pytest.param(
                "capacitor://test.vercel.app",
                ["capacitor://*.vercel.app"],
                True,
                id="wildcard_subdomains"
            ),
            pytest.param(
                "capacitor://test.vercel.app",
                ["capacitor://*.vercel.app", "capacitor://localhost"],
                True,
                id="multiple_patterns"
            ),
            pytest.param(
                "http://librarysimplified.org",
                ["https://librarysimplified.org"],
                False,
                id="scheme_mismatch"
            ),
            pytest.param(
                "https://librarysimplified.org",
                ["http://librarysimplified.org", "https://librarysimplified.org"],
                True,
                id="multiple_patterns"
            ),
            pytest.param(
                "https://www.librarysimplified.org",
                ["https://librarysimplified.org"],
                False,
                id="mismatched_subdomain_and_root"
            ),
            pytest.param(
                "https://alpha.bravo.librarysimplified.org",
                ["https://*.bravo.librarysimplified.org"],
                True,
                id="wildcard_subdomains"
            ),
            pytest.param(
                "/some/path/not/a/url",
                ["https://librarysimplified.org"],
                False,
                id="bad_candidate_value"
            ),
            pytest.param(
                "https://librarysimplified.org",
                ["/some/path/not/a/url", "https://librarysimplified.org"],
                True,
                id="bad_pattern_value_ignored"
            ),
            pytest.param(
                "https://librarysimplified.org",
                ["https://*.librarysimplified.org"],
                False,
                id="wildcard_should_not_match_root_domain"
            ),
        ]
    )
    def test_url_match_in_domain_list(self, candidate, url_list, result):
        """
        GIVEN: A candidate URL and a list of URL patterns
        WHEN:  URLUtility.url_match_in_domain_list() is called on those values
        THEN:  A boolean value should be returned, indicating whether the candidate value
               matches any pattern in the supplied list of URL patterns
        """
        assert URLUtility.url_match_in_domain_list(candidate, url_list) is result
