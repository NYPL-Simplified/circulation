from six.moves.urllib.parse import urlparse, ParseResult, urlencode


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
