
class AdminAuthenticationProvider(object):
    def __init__(self, integration):
        self.integration = integration

    def sign_in_template(self, redirect_url):
        # Returns HTML to be rendered on the sign in page for
        # this authentication provider.
        raise NotImplementedError()

    def active_credentials(self, admin):
        # Returns True if the admin's credentials are not expired.
        raise NotImplementedError()
