import datetime
import json
from lxml import etree
from nose.tools import (
    eq_, 
    assert_raises,
    assert_raises_regexp,
    set_trace,
)
import os
from StringIO import StringIO


from core.model import (
    DataSource,
    Edition,
    Identifier,
    Subject,
    Contributor,
    LicensePool,
)

from core.metadata_layer import (
    Metadata,
    CirculationData,
    IdentifierData,
    ContributorData,
    SubjectData,
)


from api.oneclick import (
    OneClickAPI,
    MockOneClickAPI,
)


from . import (
    DatabaseTest,
    #sample_data
)

'''
from api.circulation import (
    LoanInfo,
    HoldInfo,
    FulfillmentInfo,
)

from api.circulation_exceptions import *

from core.analytics import Analytics
'''


class OneClickAPITest(DatabaseTest):

    def setup(self, _db=None):
        super(OneClickAPITest, self).setup()
        self.api = MockOneClickAPI(self._db)
        #self.api = OneClickAPI(self._db)
        base_path = os.path.split(__file__)[0]
        self.resource_path = os.path.join(base_path, "files", "oneclick")


    def get_data(self, filename):
        # returns contents of sample file as string and as dict
        path = os.path.join(self.resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)



class TestOneClickAPI(OneClickAPITest):

    def test_get_patron_internal_id(self):
        datastr, datadict = self.get_data("response_patron_internal_id_not_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='9305722621')
        eq_(None, oneclick_patron_id)

        datastr, datadict = self.get_data("response_patron_internal_id_error.json")
        self.api.queue_response(status_code=500, content=datastr)
        assert_raises_regexp(
            ValueError, "Got status code 500 from external server, cannot continue.", 
            self.api.get_patron_internal_id, patron_cardno='130572262x'
        )

        datastr, datadict = self.get_data("response_patron_internal_id_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        oneclick_patron_id = self.api.get_patron_internal_id(patron_cardno='1305722621')
        eq_(939982, oneclick_patron_id)


    def test_get_patron_information(self):
        datastr, datadict = self.get_data("response_patron_info_not_found.json")
        self.api.queue_response(status_code=404, content=datastr)
        response_dictionary = self.api.get_patron_information(patron_id='939987')
        eq_(404, response_dictionary['error_code'])
        eq_(u'Patron does not exist.', response_dictionary['message'])
        
        datastr, datadict = self.get_data("response_patron_info_error.json")
        self.api.queue_response(status_code=400, content=datastr)
        response_dictionary = self.api.get_patron_information(patron_id='939982fdsfdsf')
        eq_(400, response_dictionary['error_code'])
        eq_(u'The request is invalid.', response_dictionary['message'])

        datastr, datadict = self.get_data("response_patron_info_found.json")
        self.api.queue_response(status_code=200, content=datastr)
        response_dictionary = self.api.get_patron_information(patron_id='939982')
        eq_(u'1305722621', response_dictionary['libraryCardNumber'])
        eq_(u'****', response_dictionary['libraryPin'])
        eq_(u'Mic', response_dictionary['firstName'])
        eq_(u'Mouse', response_dictionary['lastName'])
        eq_(u'mickeymouse1', response_dictionary['userName'])
        eq_(u'mickey1@mouse.com', response_dictionary['email'])


    def test_checkout_item(self):
        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )
        datastr, datadict = self.get_data("response_checkout_success.json")
        self.api.queue_response(status_code=200, content=datastr)

        patron = self.default_patron
        patron.oneclick_id = 939981

        response_dictionary = self.api.checkout_item(patron.oneclick_id, edition.primary_identifier.identifier)
        assert('error_code' not in response_dictionary)
        eq_("9781441260468", response_dictionary['isbn'])
        eq_("SUCCESS", response_dictionary['output'])
        eq_(False, response_dictionary['canRenew'])
        #eq_(9828517, response_dictionary['transactionId'])
        eq_(939981, response_dictionary['patronId'])
        eq_(1931, response_dictionary['libraryId'])

        datastr, datadict = self.get_data("response_checkout_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        response_dictionary = self.api.checkout_item(patron.oneclick_id, edition.primary_identifier.identifier)
        eq_(409, response_dictionary['error_code'])
        assert(response_dictionary['message'] in [u'Checkout item already exists', u'Title is not available for checkout'])


    def test_return_item(self):
        edition, pool = self._edition(
            identifier_type=Identifier.ONECLICK_ID,
            data_source_name=DataSource.ONECLICK,
            with_license_pool=True, 
            identifier_id = '9781441260468'
        )

        datastr, datadict = self.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=200, content="")

        patron = self.default_patron
        patron.oneclick_id = 939981

        response_dictionary = self.api.return_item(patron.oneclick_id, edition.primary_identifier.identifier)
        eq_("SUCCESS", response_dictionary['output'])
        assert('error_code' not in response_dictionary)


        datastr, datadict = self.get_data("response_return_unavailable.json")
        self.api.queue_response(status_code=409, content=datastr)
        response_dictionary = self.api.return_item(patron.oneclick_id, edition.primary_identifier.identifier)
        eq_(409, response_dictionary['error_code'])
        eq_(u'Checkout does not exists or it is already terminated or expired.', response_dictionary['message'])
        



