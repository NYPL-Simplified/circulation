"""Interface to NYPL's shadow catalog API."""
from collections import defaultdict
import json
import requests
import re
from nose.tools import set_trace

from core.config import Configuration
from core.model import (
    DataSource,
    Identifier,
    Subject,
)
from core.metadata_layer import (
    Metadata,
    ContributorData,
    LinkData,
    SubjectData,
    IdentifierData,
)

class ShadowCatalogAPI(object):

    SERVICE_NAME = "Shadowcat"

    # 000: ???
    # 001: Control Number
    # 005: Date and time of last transaction (~ last update)
    # 006: Fixed-length data elements - Additional Material Characteristics
    # 007: ??? y='cr cnu---unuuu'
    # 008: Fixed-Length Data Elements: http://loc.gov/marc/authority/ad008.html
    # 010: Library of Congress Control Number
    # 016: National Bibliographic Agency Control Number
    # 019: Merged OCLC number
    # 020: ISBN
    # 028: Publisher Number
    # 041: Language code
    # 042: Authentication code (do not use apptly)
    # 043: Geographical code
    # 049: Local holdings
    # 050: LC classification
    # 072: Subject Category Code (?) http://www.oclc.org/bibformats/en/0xx/072.html x=type, a=identifier
    # 082: DDC classification
    # 084: Other Classification (e.g. BISACSH)
    # 09X: Local call numbers
    # 100: Contributor (potentially useful)
    # 240: Uniform Title
    # 245: Title statement (potentially useful)
    # 246: Varying form of title
    # 250: Edition statement
    # 260: Publication/distribution info (includes date)
    # 263: Projected publication date
    # 264: Production notice
    # 300: Physical description (includes page count)
    # 306: Playing time
    # 336: Content type (potentially useful)
    # 337: Media type (potentially useful)
    # 338: Carrier type (physical media)
    # 347: Digital File Characteristics
    # 380: Form of Work
    # 385: Audience Characteristics (potentially useful)
    # 490: Series statement
    # 500: General note
    # 504: Bibliography, Etc.
    # 505: Formatted Contents (TOC)
    # 511: Participant or performer note
    # 520: Description! (very useful)
    # 521: TARGET AUDIENCE NOTE
    # 533: Reproduction note
    # 538: System details note
    # 588: Description source
    # 700: Personal name (non-author contributor)
    # 710: Corporate name (e.g. Overdrive)
    # 730: Added Entry - Uniform Title
    # 776: Additional physical form (contains ISBN, OCLC)
    # 800: Series Added Entry - Personal Name (who is this? primary author?)
    # 830: Series Added Entry - Uniform Title (v=series number)
    # 852: Location
    # 856: Electronic Location and Access (cover & permalink)
    # 901: OCLC number? namespace?
    # 908: LC classification
    # 909: ???
    # 921: ???
    # 945: ???
    # 946: ???
    # 969: ???
    # 995: ???


    def __init__(self):
        self.base_url = Configuration.integration_url(self.SERVICE_NAME)

    def url(self, type, identifier):
        if type == Identifier.ISBN:
            endpoint = 'isbnworks'
        elif type in (
                Identifier.THREEM_ID, 
                Identifier.OVERDRIVE_ID,
                Identifier.AXIS_360_ID,
        ):
            endpoint = 'stocknumber'
        elif type == Identifier.OCLC_WORK:
            endpoint = 'owi'
        elif type == 'bnumber':
            endpoint = 'bnumber'
        else:
            raise ValueError(
                "Cannot look up identifier of type %s" % type
            )

        if type == Identifier.OVERDRIVE_ID:
            identifier = identifier.upper()
        return self.base_url + endpoint + "/" + identifier

    def lookup(self, type, identifier):
        url = self.url(type, identifier)
        response = requests.get(url)
        return Representation.to_metadata(json.loads(response.content))

class MarcTag(object):
    def __init__(self, d):
        self.raw = dict(d)
        self.subfields_raw = d.get('subfields', [])
        self.subfields = dict()
        self.subfields_contents = dict()
        for sf in self.subfields_raw:
            key = sf.get('tag', None)
            content = sf.get('content', None)
            if content:
                self.subfields_contents[key] = content
            self.subfields[key] = sf

    def __getattr__(self, attr):
        if attr in self.raw:
            return self.raw[attr]
        if attr in self.subfields_contents:
            return self.subfields_contents[attr]
        return None

    def __repr__(self):
        return "<MarcTag: %s>" % self.raw

class Representation(object):

    from collections import Counter
    tag_type = Counter()

    @classmethod
    def to_metadata(cls, representation):
        if isinstance(representation, dict):
            representation = [representation]
        return [cls(product).metadata for product in representation]

    marc_037_b_to_identifier_type = {
        "OverDrive, Inc." : Identifier.OVERDRIVE_ID,
        "3M Cloud Library" : Identifier.THREEM_ID,
    }

    marc_035_a_to_identifier_type = {
        re.compile(r'\(OCoLC\)([0-9]+)\b') : Identifier.OCLC_NUMBER,
    }

    shadowcat_subject_type_to_native_type = {
        "bisacsh": Subject.BISAC,
        "fast": Subject.FAST,
    }

    isbn_res = [
        re.compile(r'^([0-9]{13})\b'),
        re.compile(r'^([0-9]{10})\b')
    ]

    known_vars = set([
        None, '000', '001', '003', '005', '006', '007', '008', 
        '010', '019', '020', '024', '028',
        '035', '037', '040', '041', '042', '043',
        '049', '050', '072', '082', '091', '100',
        '240', '245', '246', '250', '260', '263', 
        '264', '300', '306', '347', '336', '337', '338', 
        '380', '385', '490', '500', 
        '504', '505', '511', '520', '521', '533',
        '588', '650', '651', '652', '653', '654', '655', '700',
        '710', '730', '776', '800', '830', '856', '901', '908', 
        '909', '921', '945', '946', '969', '995',
    ])

    # These have proven to be completely useless and/or misleading.
    audience_blacklist = [
        'general', '7 years and up'
    ]

    def tags(self, key):
        return self.var.get(str(key), [])

    def __init__(self, product):
        self.subjects = []
        self.identifiers = []
        self.contributors = []
        self.links = []
        self.product = product
        self.var = defaultdict(list)
        self.unrecognized_tags = dict()
        self.title = None
        owi = self.product.get('classify:owi', None)
        if owi is not None:
            self.identifiers.append(
                IdentifierData(type=Identifier.OCLC_WORK, identifier=str(owi))
            )
        for f in self.product.get('varFields', []):
            marctag = MarcTag(f) 
            self.var[marctag.marcTag].append(marctag)

        # Find a title.
        for num in ('245', '240'):
            for tag in self.tags(num):
                self.title = tag.a
                if self.title:
                    break
            if self.title:
                break

        # Contributors
        for tag in self.tags('100'):
            role = tag.e or 'author.'
            sort_name = tag.a
            self.contributors.append(
                ContributorData(sort_name=sort_name, roles=[role])
            )

        # Subjects
        for number in ('050', '908'):
            for tag in self.tags(number):
                # Library of Congress classification
                if tag.a:
                    self.subjects.append(
                        SubjectData(type=Subject.LCC, identifier=tag.a)
                    )
                # TODO: tag.b ("Pap 2014eb") includes potentially useful
                # date information.

        for tag in self.tags('856'):
            if tag.subfields.get('3', {}).get('content') == 'Image':
                continue
            if tag.u:
                if tag.y == 'Access eNYPL' or tag.z == 'Access eNYPL':
                    self.links.append(
                        LinkData(rel='alternate', href=tag.u)
                    )

        for tag in self.tags('082'):
            if tag.a:
                self.subjects.append(
                    SubjectData(type=Subject.DDC, identifier=tag.a)
                )                

        for v in range(650, 656):
            for tag in self.tags(v):
                type = getattr(tag, '2', None)
                native_type = Subject.TAG
                if type:
                    if type.endswith('.'):
                        type = type[:-1]
                    Representation.tag_type[type] += 1
                    native_type = self.shadowcat_subject_type_to_native_type.get(
                        type, Subject.TAG
                    )

                identifiers = [x for x in [tag.a, tag.v] if x]
                for identifier in identifiers: 
                    self.subjects.append(
                        SubjectData(type=native_type, identifier=identifier)
                    )

        # Identifiers
        for tag in self.tags('037'):
            if tag.a and (tag.b in self.marc_037_b_to_identifier_type):
                t = self.marc_037_b_to_identifier_type[tag.b]
                self.identifiers.append(
                    IdentifierData(type=t, identifier=tag.a)
                )

        for tag in self.tags('020'):
            isbn = tag.a
            if not isbn:
                continue
            for r in self.isbn_res:
                m = r.search(isbn)
                if m:
                    isbn = m.groups()[0] 
                    self.identifiers.append(
                        IdentifierData(type=Identifier.ISBN, identifier=isbn)
                    )

        for key in ['385', '521']:
            for tag in self.tags(key):
                identifier = tag.a
                if identifier.lower() in self.audience_blacklist:
                    continue
                self.subjects.append(
                    SubjectData(
                        type=Subject.FREEFORM_AUDIENCE, identifier=identifier
                    )
                )

        for tag in self.tags('035'):
            potential = tag.a
            if not potential:
                continue
            identifier = None
            for r, type in self.marc_035_a_to_identifier_type.items():
                m = r.search(potential)
                if m:
                    identifier = m.groups()[0]
                    break
            if identifier:
                self.identifiers.append(
                    IdentifierData(type=type, identifier=identifier)
                )                

        # Keep track of items we haven't seen before.
        for key, var in self.var.items():
            if key not in self.known_vars:
                self.unrecognized_tags[key] = var

    @property
    def metadata(self):
        return Metadata(
            data_source=DataSource.NYPL_SHADOWCAT,
            title=self.title,
            identifiers=self.identifiers, 
            subjects=self.subjects,
            links=self.links,
        )

