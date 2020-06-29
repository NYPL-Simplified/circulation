"""Vendor-specific variants of the standard Web Publication Manifest classes.
"""

from nose.tools import set_trace
from core.model import (
    DeliveryMechanism,
    Representation,
)
from core.util.web_publication_manifest import AudiobookManifest

class SpineItem(object):
    """Metadata about a piece of playable audio from an audiobook."""

    def __init__(self, title, duration, part, sequence,
                 media_type=Representation.MP3_MEDIA_TYPE):
        """Constructor.

        :param title: The title of this spine item.
        :param duration: The duration of this spine item, in milliseconds.
        :param part: The part number of this spine item, roughly equivalent
           to 'Part X' in a book.
        :param sequence: The sequence number of this spine item within its
           part, roughly equivalent to a chapter number.
        :param media_type: The media type of this spine item.
        """
        self.title = title
        self.duration = duration
        self.part = part
        self.sequence = sequence
        self.media_type = media_type

    @classmethod
    def sort_key(self, o):
        """Used to sort a list of SpineItem objects in reading
        order.
        """
        return (o.part, o.sequence)


class FindawayManifest(AudiobookManifest):

    # This URI prefix makes it clear when we are using a term coined
    # by Findaway in a JSON-LD document.
    FINDAWAY_EXTENSION_CONTEXT = "http://librarysimplified.org/terms/third-parties/findaway.com/"

    MEDIA_TYPE = DeliveryMechanism.FINDAWAY_DRM

    def __init__(
        self, license_pool, accountId=None, checkoutId=None,
        fulfillmentId=None, licenseId=None, sessionKey=None,
        spine_items=[]
    ):
        """Create a FindawayManifest object from raw data.

        :param license_pool: A LicensePool for the title being fulfilled.
        This will be used to fill in basic bibliographic information.

        :param accountId: An opaque string that Findaway calls the
        'account ID'. Apparently this is no longer used.

        :param checkoutId: An opaque string that Findaway calls the
        'checkout transaction ID'. Apparently this is no longer used.

        :param fulfillmentId: An opaque string that Findaway calls the
        'title identifier' or 'content ID'.

        :param licenseId: An opaque string that Findaway calls the
        'license ID'

        :param sessionId: An opaque string that Findaway calls the
        'session key'.

        :param spine_items: A list of SpineItem objects representing
        the chapters or other sections of the audiobook.

        The PEP8-incompatible variable names are for compatibility
        with the names of these variables in the JSON-LD documents.
        """

        context_with_extension = [
            "http://readium.org/webpub/default.jsonld",
            {"findaway" : self.FINDAWAY_EXTENSION_CONTEXT},
        ]
        super(FindawayManifest, self).__init__(context=context_with_extension)

        # Add basic bibliographic information (identifier, title,
        # cover link) to the manifest based on our existing knowledge
        # of the LicensePool and its Work.
        self.update_bibliographic_metadata(license_pool)

        # Add Findaway-specific DRM information as an 'encrypted' object
        # within the metadata object.
        encrypted = dict(
            scheme='http://librarysimplified.org/terms/drm/scheme/FAE'
        )
        self.metadata['encrypted'] = encrypted
        for findaway_extension, value in [
                ('accountId', accountId),
                ('checkoutId', checkoutId),
                ('fulfillmentId', fulfillmentId),
                ('licenseId', licenseId),
                ('sessionKey', sessionKey)
        ]:
            if not value:
                continue
            output_key = 'findaway:' + findaway_extension
            encrypted[output_key] = value

        # Add the SpineItems as reading order items. None of them will
        # have working 'href' fields -- it's just to give the client a
        # picture of the structure of the timeline.
        part_key = 'findaway:part'
        sequence_key = 'findaway:sequence'
        total_duration = 0
        spine_items.sort(key=SpineItem.sort_key)
        for item in spine_items:
            kwargs = {
                part_key: item.part,
                sequence_key: item.sequence
            }
            self.add_reading_order(
                href=None, title=item.title, duration=item.duration,
                type=item.media_type, **kwargs
            )
            total_duration += item.duration

        if spine_items:
            self.metadata['duration'] = total_duration

