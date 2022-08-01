from core.util.webpub_manifest_parser.core.ast import LinkProperties, Metadata, Node
from core.util.webpub_manifest_parser.core.parsers import ArrayParser, EnumParser
from core.util.webpub_manifest_parser.core.properties import (
    EnumProperty,
    IntegerProperty,
    Property,
    StringProperty,
    TypeProperty,
    URIProperty,
)


class EPUBPresentationHints(Node):
    """EPUB presentation hints."""

    layout = EnumProperty("layout", required=False, items=["fixed", "reflowable"])


class EPUBMetadata(Metadata):
    """EPUB metadata."""

    presentation = TypeProperty(
        "presentation", required=False, nested_type=EPUBPresentationHints
    )


class EPUBEncryptionSettings(Node):
    """EPUB encryption settings."""

    algorithm = URIProperty("algorithm", required=True)
    compression = StringProperty("compression", required=False)
    original_length = IntegerProperty("originalLength", required=False)
    profile = URIProperty("profile", required=False)
    scheme = URIProperty("scheme", required=False)


class EPUBLinkProperties(LinkProperties):
    """EPUB link properties."""

    contains = Property(
        "contains",
        required=False,
        parser=ArrayParser(
            item_parser=EnumParser(
                ["mathml", "onix", "remote-resources", "js", "svg", "xmp"]
            ),
            unique_items=True,
        ),
    )
    layout = EnumProperty("layout", False, ["fixed", "reflowable"])
    encryption_settings = TypeProperty("encrypted", False, EPUBEncryptionSettings)
