from nose.tools import set_trace
from collections import defaultdict
import datetime
import base64
import os
import json
import logging
import re

from config import (
    Configuration, 
    temp_config,
)

from util.http import (
    HTTP,
    RemoteIntegrationException,
)

from model import (
    Identifier,
)

from config import Configuration
from coverage import BibliographicCoverageProvider


class OneClickAPI(object):

    API_VERSION = "v1"

    DATE_FORMAT = "%m-%d-%Y"

    # a complete response returns the json structure with more data fields than a basic response does
    RESPONSE_VERBOSITY = {0:'basic', 1:'compact', 2:'complete', 3:'extended', 4:'hypermedia'}

    log = logging.getLogger("OneClick API")

    def __init__(self, _db, library_id=None, username=None, password=None, 
        remote_stage=None, base_url=None, basic_token=None):
        self._db = _db
        (env_library_id, env_username, env_password, 
         env_remote_stage, env_base_url, env_basic_token) = self.environment_values()
            
        self.library_id = library_id or env_library_id
        self.username = username or env_username
        self.password = password or env_password
        self.remote_stage = remote_stage or env_remote_stage
        self.base_url = base_url or env_base_url
        self.base_url = self.base_url + self.API_VERSION + '/'
        self.token = basic_token or env_basic_token


    @classmethod
    def environment_values(cls):
        config = Configuration.integration('OneClick')
        values = []
        for name in [
                'library_id',
                'username',
                'password',
                'remote_stage', 
                'url', 
                'basic_token'
        ]:
            value = config.get(name)
            if value:
                value = value.encode("utf8")
            values.append(value)
        return values


    @classmethod
    def from_environment(cls, _db):
        # Make sure all environment values are present. If any are missing,
        # return None
        values = cls.environment_values()
        if len([x for x in values if not x]):
            cls.log.info(
                "No OneClick client configured."
            )
            return None
        return cls(_db)


    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.ONE_CLICK)


    @property
    def authorization_headers(self):
        authorization = self.token
        authorization = authorization.encode("utf_16_le")
        authorization = base64.b64encode(authorization)
        return dict(Authorization="Basic " + authorization)


    def request(self, url, method='get', extra_headers={}, data=None,
                params=None, verbosity='complete'):
        """Make an HTTP request, acquiring/refreshing a bearer token
        if necessary.
        """
        if verbosity not in self.RESPONSE_VERBOSITY.values():
            verbosity = self.RESPONSE_VERBOSITY[2]

        headers = dict(extra_headers)
        headers['Authorization'] = "Basic " + self.token
        headers['Content-Type'] = 'application/json'
        headers['Accept-Media'] = verbosity

        # for now, do nothing with error codes, but in the future might have some that 
        # will warrant repeating the request.
        disallowed_response_codes = ["409"]
        response = self._make_request(
            url=url, method=method, headers=headers,
            data=data, params=params, 
            disallowed_response_codes=disallowed_response_codes
        )
        
        return response


    def search(self, mediatype='ebook', genres=[], audience=None, availability=None, author=None, title=None, 
        page_size=100, page_index=None, verbosity=None): 
        """
        Form a rest-ful search query, send to OneClick, and obtain the results.

        :param mediatype Facet to limit results by media type.  Options are: "eaudio", "ebook".
        :param genres The books found lie at intersection of genres passed.
        :audience Facet to limit results by target age group.  Options include (there may be more): "adult", 
            "beginning-reader", "childrens", "young-adult".
        :param availability Facet to limit results by copies left.  Options are "available", "unavailable", or None
        :param author Full name to search on.
        :param author Book title to search on.
        :param page_index Used for paginated result sets.  Zero-based.
        :param verbosity "basic" returns smaller number of response json lines than "complete", etc..
        """
        url = self.base_url + "libraries/" + self.library_id + "/search" 

        # make sure availability is in allowed format
        if availability not in ("available", "unavailable"):
            availability = None

        args = dict()
        if mediatype:
            args['media-type'] = mediatype
        if genres:
            args['genre'] = genres
        if audience:
            args['audience'] = audience
        if availability:
            args['availability'] = availability
        if author:
            args['author'] = author
        if title:
            args['title'] = title
        if page_size != 100:
            args['page-size'] = page_size
        if page_index:
            args['page-index'] = page_index

        response = self.request(url, params=args, verbosity=verbosity)
        return response



    def get_all_available_through_search(self):
        """
        Gets a list of ebook and eaudio items this library has access to, that are currently
        available to lend.  Uses the "availability" facet of the search function.
        An alternative to self.get_availability_info().
        Calls paged search until done.
        Uses minimal verbosity for result set.

        Note:  Some libraries can see other libraries' catalogs, even if the patron 
        cannot checkout the items.  The library ownership information is in the "interest" 
        fields of the response.

        :return A dictionary representation of the response, containing catalog count and ebook item - interest pairs.
        """
        page = 0;
        response = self.search(availability='available', verbosity=self.RESPONSE_VERBOSITY[0])

        respdict = response.json()
        if not respdict:
            raise IOError("OneClick availability response not parseable - has no respdict.")

        if not ('pageIndex' in respdict and 'pageCount' in respdict):
            raise IOError("OneClick availability response not parseable - has no page counts.")
        page_index = respdict['pageIndex']
        page_count = respdict['pageCount']

        while (page_count > (page_index+1)):
            page_index += 1
            response = self.search(availability='available', verbosity=self.RESPONSE_VERBOSITY[0], page_index=page_index)
            tempdict = response.json()
            if not ('items' in tempdict):
                raise IOError("OneClick availability response not parseable - has no next dict.")
            item_interest_pairs = tempdict['items']
            respdict['items'].append(item_interest_pairs)

        return respdict


    def get_ebook_availability_info(self):
        """
        Gets a list of ebook items this library has access to, through the "availability" endpoint.
        The response at this endpoint is laconic -- just enough fields per item to 
        identify the item and declare it either available to lend or not.

        :return A list of dictionary items, each item giving "yes/no" answer on a book's current availability to lend.
        Example of returned item format:
            "timeStamp": "2016-10-07T16:11:52.5887333Z"
            "isbn": "9781420128567"
            "mediaType": "eBook"
            "availability": false
            "titleId": 39764
        """
        url = self.base_url + "libraries/" + self.library_id + "/media/ebook/availability" 

        args = dict()

        response = self.request(url)

        resplist = response.json()
        if not resplist:
            raise IOError("OneClick availability response not parseable - has no resplist.")

        return resplist


    @classmethod
    def create_identifier_strings(cls, identifiers):
        identifier_strings = []
        for i in identifiers:
            if isinstance(i, Identifier):
                value = i.identifier
            else:
                value = i
            identifier_strings.append(value)

        return identifier_strings


    def _make_request(self, url, method, headers, data=None, params=None, **kwargs):
        print "url= %s" % url
        print "params= %s" % params
        print "kwargs= %s" % kwargs

        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(
            method, url, headers=headers, data=data,
            params=params, **kwargs
        )



class MockOneClickAPI(OneClickAPI):

    def __init__(self, _db, with_token=True, *args, **kwargs):
        with temp_config() as config:
            config[Configuration.INTEGRATIONS]['OneClick'] = {
                'library_id' : 'library_id_123',
                'username' : 'username_123',
                'password' : 'password_123',
                'server' : 'http://axis.test/',
                'remote_stage' : 'qa', 
                'url' : 'www.oneclickapi.test', 
                'basic_token' : 'abcdef123hijklm'
            }
            super(MockOneClickAPI, self).__init__(_db, *args, **kwargs)
        if with_token:
            self.token = "mock token"
        self.responses = []
        self.requests = []


    def queue_response(self, status_code, headers={}, content=None):
        from testing import MockRequestsResponse
        self.responses.insert(
            0, MockRequestsResponse(status_code, headers, content)
        )


    def _make_request(self, url, *args, **kwargs):
        self.requests.append([url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, kwargs.get('allowed_response_codes'),
            kwargs.get('disallowed_response_codes')
        )









