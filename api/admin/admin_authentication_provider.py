from nose.tools import set_trace

class AdminAuthenticationProvider(object):
    def __init__(self, integration):
        self.integration = integration

    def auth_uri(self, redirect_url):
        # Returns a URI that an admin can use to log in with this
        # authentication provider.
        raise NotImplementedError()

    def active_credentials(self, admin):
        # Returns True if the admin's credentials are not expired.
        raise NotImplementedError()
