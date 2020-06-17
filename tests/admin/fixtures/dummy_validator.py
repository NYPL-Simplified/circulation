from api.admin.validator import Validator as BaseValidator


class DummyAuthenticationProviderValidator(BaseValidator):
    pass


Validator = DummyAuthenticationProviderValidator
