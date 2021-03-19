from flask_babel import lazy_gettext as _
from core.util.problem_detail import ProblemDetail
from api.config import Configuration
from api.circulation_exceptions import *
from api.problem_details import *
from core.testing import DatabaseTest


class TestCirculationExceptions(object):
    def test_as_problem_detail_document(self):
        """Verify that circulation exceptions can be turned into ProblemDetail
        documents.
        """

        e = RemoteInitiatedServerError("message", "some service")
        doc = e.as_problem_detail_document()
        assert "Integration error communicating with some service" == doc.detail

        e = AuthorizationExpired()
        assert EXPIRED_CREDENTIALS == e.as_problem_detail_document()

        e = AuthorizationBlocked()
        assert BLOCKED_CREDENTIALS == e.as_problem_detail_document()

        e = PatronHoldLimitReached()
        assert HOLD_LIMIT_REACHED == e.as_problem_detail_document()

        e = NoLicenses()
        assert NO_LICENSES == e.as_problem_detail_document()


class TestLimitReached(DatabaseTest):
    """Test LimitReached, which may send different messages depending on the value of a
    library ConfigurationSetting.
    """

    def test_as_problem_detail_document(self):
        generic_message = _("You exceeded the limit, but I don't know what the limit was.")
        pd = ProblemDetail(
            "http://uri/",
            403,
            _("Limit exceeded."),
            generic_message
        )
        setting = "some setting"

        class Mock(LimitReached):
            BASE_DOC = pd
            SETTING_NAME = setting
            MESSAGE_WITH_LIMIT = _("The limit was %(limit)d.")

        # No limit -> generic message.
        ex = Mock(library=self._default_library)
        pd = ex.as_problem_detail_document()
        assert None == ex.limit
        assert generic_message == pd.detail

        # Limit but no library -> generic message.
        self._default_library.setting(setting).value = 14
        ex = Mock()
        assert None == ex.limit
        pd = ex.as_problem_detail_document()
        assert generic_message == pd.detail

        # Limit and library -> specific message.
        ex = Mock(library=self._default_library)
        assert 14 == ex.limit
        pd = ex.as_problem_detail_document()
        assert "The limit was 14." == pd.detail

    def test_subclasses(self):
        # Use end-to-end tests to verify that the subclasses of
        # LimitReached define the right constants.
        library = self._default_library

        library.setting(Configuration.LOAN_LIMIT).value = 2
        pd = PatronLoanLimitReached(library=library).as_problem_detail_document()
        assert ("You have reached your loan limit of 2. You cannot borrow anything further until you return something." ==
            pd.detail)

        library.setting(Configuration.HOLD_LIMIT).value = 3
        pd = PatronHoldLimitReached(library=library).as_problem_detail_document()
        assert ("You have reached your hold limit of 3. You cannot place another item on hold until you borrow something or remove a hold." ==
            pd.detail)
