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

from util import LanguageCodes
#from util.xmlparser import XMLParser
from util.jsonparser import JsonParser
from util.http import (
    HTTP,
    RemoteIntegrationException,
)
from coverage import CoverageFailure
from model import (
    Contributor,
    DataSource,
    DeliveryMechanism,
    LicensePool,
    Edition,
    Identifier,
    Representation,
    Subject,
)

from metadata_layer import (
    SubjectData,
    ContributorData,
    FormatData,
    IdentifierData,
    CirculationData,
    Metadata,
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


    '''
    library delta:
    http://api.oneclickdigital.us/v1/libraries/1998/media/delta?begin=2016-07-26&end=2016-09-26
    get

    get whole catalog
    https://api.oneclickdigital.us/v1/libraries/1931/media/all
    get


    http://api.oneclickdigital.us/v1/libraries/1931/media/9780307378101

    '''

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



class OneClickBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for OneClick records.

    """
    def __init__(self, _db, input_identifier_types=None, 
                 metadata_replacement_policy=None, oneclick_api=None,
                 **kwargs):
        # We ignore the value of input_identifier_types, but it's
        # passed in by RunCoverageProviderScript, so we accept it as
        # part of the signature.
        self.parser = BibliographicParser()
        oneclick_api = oneclick_api or OneClickAPI(_db)
        super(OneClickBibliographicCoverageProvider, self).__init__(
            _db, oneclick_api, DataSource.ONE_CLICK,
            batch_size=25, 
            metadata_replacement_policy=metadata_replacement_policy,
            **kwargs
        )


    def process_batch(self, identifiers):
        identifier_strings = self.api.create_identifier_strings(identifiers)
        response = self.api.availability(title_ids=identifier_strings)
        seen_identifiers = set()
        batch_results = []
        for metadata, availability in self.parser.process_all(response.content):
            identifier, is_new = metadata.primary_identifier.load(self._db)
            if not identifier in identifiers:
                # Axis 360 told us about a book we didn't ask
                # for. This shouldn't happen, but if it does we should
                # do nothing further.
                continue
            seen_identifiers.add(identifier.identifier)
            result = self.set_metadata(identifier, metadata)
            if not isinstance(result, CoverageFailure):
                result = self.handle_success(identifier)
            batch_results.append(result)

        # Create a CoverageFailure object for each original identifier
        # not mentioned in the results.
        for identifier_string in identifier_strings:
            if identifier_string not in seen_identifiers:
                identifier, ignore = Identifier.for_foreign_id(
                    self._db, Identifier.AXIS_360_ID, identifier_string
                )
                result = CoverageFailure(
                    identifier, "Book not in collection", data_source=self.output_source, transient=False
                )
                batch_results.append(result)
        return batch_results

    def handle_success(self, identifier):
        return self.set_presentation_ready(identifier)

    def process_item(self, identifier):
        results = self.process_batch([identifier])
        return results[0]



class OneClickParser(XMLParser):

    NS = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    SHORT_DATE_FORMAT = "%m/%d/%Y"
    FULL_DATE_FORMAT_IMPLICIT_UTC = "%m/%d/%Y %I:%M:%S %p"
    FULL_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p +00:00"

    def _xpath1_boolean(self, e, target, ns, default=False):
        text = self.text_of_optional_subtag(e, target, ns)
        if text is None:
            return default
        if text == 'true':
            return True
        else:
            return False

    def _xpath1_date(self, e, target, ns):
        value = self.text_of_optional_subtag(e, target, ns)
        if value is None:
            return value
        try:
            attempt = datetime.datetime.strptime(
                value, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
            value += ' +00:00'
        except ValueError:
            pass
        return datetime.datetime.strptime(value, self.FULL_DATE_FORMAT)


'''
class BibliographicParser(OneClickParser):

    DELIVERY_DATA_FOR_AXIS_FORMAT = {
        "Blio" : None,
        "Acoustik" : None,
        "ePub" : (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        "PDF" : (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
    }

    log = logging.getLogger("Axis 360 Bibliographic Parser")

    @classmethod
    def parse_list(self, l):
        """Turn strings like this into lists:

        FICTION / Thrillers; FICTION / Suspense; FICTION / General
        Ursu, Anne ; Fortune, Eric (ILT)
        """
        return [x.strip() for x in l.split(";")]

    def __init__(self, include_availability=True, include_bibliographic=True):
        self.include_availability = include_availability
        self.include_bibliographic = include_bibliographic

    def process_all(self, string):
        for i in super(BibliographicParser, self).process_all(
                string, "//axis:title", self.NS):
            yield i

    def extract_availability(self, circulation_data, element, ns):
        identifier = self.text_of_subtag(element, 'axis:titleId', ns)
        primary_identifier = IdentifierData(Identifier.AXIS_360_ID, identifier)

        if not circulation_data: 
            circulation_data = CirculationData(
                data_source=DataSource.AXIS_360, 
                primary_identifier=primary_identifier, 
            )

        availability = self._xpath1(element, 'axis:availability', ns)
        total_copies = self.int_of_subtag(availability, 'axis:totalCopies', ns)
        available_copies = self.int_of_subtag(
            availability, 'axis:availableCopies', ns)
        size_of_hold_queue = self.int_of_subtag(
            availability, 'axis:holdsQueueSize', ns)

        availability_updated = self.text_of_optional_subtag(
            availability, 'axis:updateDate', ns)
        if availability_updated:
            try:
                attempt = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT_IMPLICIT_UTC)
                availability_updated += ' +00:00'
            except ValueError:
                pass
            availability_updated = datetime.datetime.strptime(
                    availability_updated, self.FULL_DATE_FORMAT)

        circulation_data.licenses_owned=total_copies
        circulation_data.licenses_available=available_copies
        circulation_data.licenses_reserved=0
        circulation_data.patrons_in_hold_queue=size_of_hold_queue

        return circulation_data


    # Axis authors with a special role have an abbreviation after their names,
    # e.g. "San Ruby (FRW)"
    role_abbreviation = re.compile("\(([A-Z][A-Z][A-Z])\)$")
    generic_author = object()
    role_abbreviation_to_role = dict(
        INT=Contributor.INTRODUCTION_ROLE,
        EDT=Contributor.EDITOR_ROLE,
        PHT=Contributor.PHOTOGRAPHER_ROLE,
        ILT=Contributor.ILLUSTRATOR_ROLE,
        TRN=Contributor.TRANSLATOR_ROLE,
        FRW=Contributor.FOREWORD_ROLE,
        ADP=generic_author, # Author of adaptation
        COR=generic_author, # Corporate author
    )

    @classmethod
    def parse_contributor(cls, author, primary_author_found=False):
        if primary_author_found:
            default_author_role = Contributor.AUTHOR_ROLE
        else:
            default_author_role = Contributor.PRIMARY_AUTHOR_ROLE
        role = default_author_role
        match = cls.role_abbreviation.search(author)
        if match:
            role_type = match.groups()[0]
            role = cls.role_abbreviation_to_role.get(
                role_type, Contributor.UNKNOWN_ROLE)
            if role is cls.generic_author:
                role = default_author_role
            author = author[:-5].strip()
        return ContributorData(
            sort_name=author, roles=role)


    def extract_bibliographic(self, element, ns):
        """Turn bibliographic metadata into a Metadata and a CirculationData objects, 
        and return them as a tuple."""

        # TODO: These are consistently empty (some are clearly for
        # audiobooks) so I don't know what they do and/or what format
        # they're in.
        #
        # annotation
        # edition
        # narrator
        # runtime

        identifier = self.text_of_subtag(element, 'axis:titleId', ns)
        isbn = self.text_of_optional_subtag(element, 'axis:isbn', ns)
        title = self.text_of_subtag(element, 'axis:productTitle', ns)

        contributor = self.text_of_optional_subtag(
            element, 'axis:contributor', ns)
        contributors = []
        found_primary_author = False
        if contributor:
            for c in self.parse_list(contributor):
                contributor = self.parse_contributor(
                    c, found_primary_author)
                if Contributor.PRIMARY_AUTHOR_ROLE in contributor.roles:
                    found_primary_author = True
                contributors.append(contributor)

        subject = self.text_of_optional_subtag(element, 'axis:subject', ns)
        subjects = []
        if subject:
            for subject_identifier in self.parse_list(subject):
                subjects.append(
                    SubjectData(
                        type=Subject.BISAC, identifier=subject_identifier,
                        weight=1
                    )
                )

        publication_date = self.text_of_optional_subtag(
            element, 'axis:publicationDate', ns)
        if publication_date:
            publication_date = datetime.datetime.strptime(
                publication_date, self.SHORT_DATE_FORMAT)

        series = self.text_of_optional_subtag(element, 'axis:series', ns)
        publisher = self.text_of_optional_subtag(element, 'axis:publisher', ns)
        imprint = self.text_of_optional_subtag(element, 'axis:imprint', ns)

        audience = self.text_of_optional_subtag(element, 'axis:audience', ns)
        if audience:
            subjects.append(
                SubjectData(
                    type=Subject.AXIS_360_AUDIENCE,
                    identifier=audience,
                    weight=1,
                )
            )

        language = self.text_of_subtag(element, 'axis:language', ns)

        # We don't use this for anything.
        # file_size = self.int_of_optional_subtag(element, 'axis:fileSize', ns)
        primary_identifier = IdentifierData(Identifier.AXIS_360_ID, identifier)
        identifiers = []
        if isbn:
            identifiers.append(IdentifierData(Identifier.ISBN, isbn))

        formats = []
        acceptable = False
        seen_formats = []
        for format_tag in self._xpath(
                element, 'axis:availability/axis:availableFormats/axis:formatName', 
                ns
        ):
            informal_name = format_tag.text
            seen_formats.append(informal_name)
            if informal_name not in self.DELIVERY_DATA_FOR_AXIS_FORMAT:
                self.log("Unrecognized Axis format name for %s: %s" % (
                    identifier, informal_name
                ))
            elif self.DELIVERY_DATA_FOR_AXIS_FORMAT.get(informal_name):
                content_type, drm_scheme = self.DELIVERY_DATA_FOR_AXIS_FORMAT[
                    informal_name
                ]
                formats.append(
                    FormatData(content_type=content_type, drm_scheme=drm_scheme)
                )
        
        if not formats:
            self.log.error(
                "No supported format for %s (%s)! Saw: %s", identifier,
                title, ", ".join(seen_formats)
            )

        metadata = Metadata(
            data_source=DataSource.AXIS_360,
            title=title,
            language=language,
            medium=Edition.BOOK_MEDIUM,
            series=series,
            publisher=publisher,
            imprint=imprint,
            published=publication_date,
            primary_identifier=primary_identifier,
            identifiers=identifiers,
            subjects=subjects,
            contributors=contributors,
        )

        circulationdata = CirculationData(
            data_source=DataSource.AXIS_360,
            primary_identifier=primary_identifier,
            formats=formats,
        )

        metadata.circulation = circulationdata
        return metadata


    def process_one(self, element, ns):
        if self.include_bibliographic:
            bibliographic = self.extract_bibliographic(element, ns)
        else:
            bibliographic = None

        passed_availability = None
        if bibliographic and bibliographic.circulation:
            passed_availability = bibliographic.circulation

        if self.include_availability:
            availability = self.extract_availability(circulation_data=passed_availability, element=element, ns=ns)
        else:
            availability = None

        return bibliographic, availability
'''

