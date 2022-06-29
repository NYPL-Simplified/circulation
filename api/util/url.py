from urllib.parse import urlparse, ParseResult, urlencode


class URLUtility(object):
    """Contains different helper methods simplifying URL construction."""

    @staticmethod
    def build_url(base_url, query_parameters):
        """Construct a URL with specified query parameters.

        :param base_url: Base URL
        :type base_url: str

        :param query_parameters: Dictionary containing query parameters
        :type query_parameters: Dict

        :return: Constructed URL
        :rtype: str
        """
        result = urlparse(base_url)
        result = ParseResult(
            result.scheme,
            result.netloc,
            result.path,
            result.params,
            urlencode(query_parameters),
            result.fragment
        )

        return result.geturl()

    @staticmethod
    def url_match_in_domain_list(url, domain_list):
        """
        Attempts to match a candidate URL against a list of URL patterns, with wildcard
        matching of subdomains under a given root.

        To be matched against, a value in 'domain_list' must be of the form:

            `(http|https)://[(<subdomain>|*).]<domain>.<tld>`

        Examples of valid domain_list entries:

            ```
            http://librarysimplified.org
            https://librarysimplified.org
            https://www.librarysimplified.org
            https://*.librarysimplified.org
            https://alpha.bravo.charlie.librarysimplified.org
            https://*.charlie.librarysimplified.org
            capacitor://*.vercel.app
            ```

        Note that the entry `http://*.librarysimplified.org` WILL NOT match
        the URL of the root domain `http://librarysimplified.org`. To match the root
        domain you must also include it as a separate, non-wildcard entry.
        """
        try:
            url_parsed = urlparse(url)
        except AttributeError:
            return False    # origin value was not a string

        url_match_in_list = False

        for allowed_domain in domain_list:
            if url_match_in_list:
                break       # previous iteration matched

            try:
                allowed_parsed = urlparse(allowed_domain)
            except AttributeError:
                # TODO: log a warning about a bad value in the setting
                continue

            # If the scheme doesn't match it won't be allowed, period
            if url_parsed.scheme == allowed_parsed.scheme:
                # If we have a subdomain wildcard at the start of an allowed pattern,
                # check to see if the rest of the pattern is present in the candidate URL
                # Alternatively, check for a complete match of the netloc strings
                if (
                    (
                        allowed_parsed.netloc.startswith('*.')
                        and url_parsed.netloc.endswith(allowed_parsed.netloc[1:])
                    )
                    or url_parsed.netloc == allowed_parsed.netloc
                ):
                    url_match_in_list = True

        return url_match_in_list
