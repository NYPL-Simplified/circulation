from core.util.webpub_manifest_parser.errors import BaseError


class BaseSyntaxError(BaseError):
    """Exception raised in the case of syntax errors."""


class BaseSemanticError(BaseError):
    """Exception raised in the case of any semantic errors."""
