import datetime
import feedparser
from nose.tools import set_trace
from io import StringIO
from zipfile import ZipFile
from lxml import etree
import os
from flask_babel import lazy_gettext as _

from core.opds import OPDSFeed
from core.opds_import import (
    OPDSImporter,
    OPDSImportMonitor,
    OPDSXMLParser,
)
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Resource,
    Representation,
    RightsStatus,
)
from core.util.epub import EpubAccessor


class FeedbooksOPDSImporter(OPDSImporter):

    REALLY_IMPORT_KEY = 'really_import'
    REPLACEMENT_CSS_KEY = 'replacement_css'

    NAME = ExternalIntegration.FEEDBOOKS
    DESCRIPTION = _("Import open-access books from FeedBooks.")
    SETTINGS = [
        {
            "key": REALLY_IMPORT_KEY,
            "type": "select",
            "label": _("Really?"),
            "description": _("Most libraries are better off importing free Feedbooks titles via an OPDS Import integration from NYPL's open-access content server or DPLA's Open Bookshelf. This setting makes sure you didn't create this collection by accident and really want to import directly from Feedbooks."),
            "options": [
                { "key": "false", "label": _("Don't actually import directly from Feedbooks.") },
                { "key": "true", "label": _("I know what I'm doing; import directly from Feedbooks.") },
          ],
          "default": "false"
        },
        {
            "key": Collection.EXTERNAL_ACCOUNT_ID_KEY,
            "label": _("Import books in this language"),
            "description": _("Feedbooks offers separate feeds for different languages. Each one can be made into a separate collection."),
            "type": "select",
            "options": [
                { "key": "en", "label": _("English") },
                { "key": "es", "label": _("Spanish") },
                { "key": "fr", "label": _("French") },
                { "key": "it", "label": _("Italian") },
                { "key": "de", "label": _("German") },
            ],
            "default": "en",
        },
        {
            "key" : REPLACEMENT_CSS_KEY,
            "label": _("Replacement stylesheet"),
            "description": _("If you are mirroring the Feedbooks titles, you may replace the Feedbooks stylesheet with an alternate stylesheet in the mirrored copies. The default value is an accessibility-focused stylesheet produced by the DAISY consortium. If you mirror Feedbooks titles but leave this empty, the Feedbooks titles will be mirrored as-is."),
            "default": "http://www.daisy.org/z3986/2005/dtbook.2005.basic.css",
        },

    ]

    BASE_OPDS_URL = 'http://www.feedbooks.com/books/recent.atom?lang=%(language)s'

    THIRTY_DAYS = datetime.timedelta(days=30)

    def __init__(self, _db, collection, *args, **kwargs):
        integration = collection.external_integration
        new_css_url = integration.setting(self.REPLACEMENT_CSS_KEY).value
        if new_css_url:
            # We may need to modify incoming content to replace CSS.
            kwargs['content_modifier'] = self.replace_css
        kwargs['data_source_name'] = DataSource.FEEDBOOKS

        really_import = integration.setting(self.REALLY_IMPORT_KEY).bool_value
        if not really_import:
            raise Exception("Refusing to instantiate a Feedbooks importer because it's configured to not actually do an import.")

        self.language = collection.external_account_id

        super(FeedbooksOPDSImporter, self).__init__(_db, collection, **kwargs)

        self.new_css = None
        if new_css_url and self.http_get:
            status_code, headers, content = self.http_get(new_css_url, {})
            if status_code != 200:
                raise IOError(
                    "Replacement stylesheet URL returned %r response code." % status_code
                )
            content_type = headers.get('content-type', '')
            if not content_type.startswith('text/css'):
                raise IOError(
                    "Replacement stylesheet is %r, not a CSS document." % content_type
                )
            self.new_css = content


    def extract_feed_data(self, feed, feed_url=None):
        metadata, failures = super(FeedbooksOPDSImporter, self).extract_feed_data(
            feed, feed_url
        )
        for id, m in list(metadata.items()):
            self.improve_description(id, m)
        return metadata, failures

    @classmethod
    def rights_uri_from_feedparser_entry(cls, entry):
        """(Refuse to) determine the URI that best encapsulates the rights
        status of the downloads associated with this book.

        We cannot answer this question from within feedparser code; we have
        to wait until we enter elementtree code.
        """
        return None

    @classmethod
    def rights_uri_from_entry_tag(cls, entry):
        """Determine the URI that best encapsulates the rights
        status of the downloads associated with this book.
        """
        rights = OPDSXMLParser._xpath1(entry, 'atom:rights')
        if rights is not None:
            rights = rights.text
        source = OPDSXMLParser._xpath1(entry, 'dcterms:source')
        if source is not None:
            source = source.text
        publication_year = OPDSXMLParser._xpath1(entry, 'dcterms:issued')
        if publication_year is not None:
            publication_year = publication_year.text
        return RehostingPolicy.rights_uri(rights, source, publication_year)

    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None, do_get=None):
        """Determine a more accurate value for this entry's default rights
        URI.

        We can't get it right within the Feedparser code, because
        dcterms:issued (which we use to determine whether a work is
        public domain in the United States) is not available through
        Feedparser.
        """
        detail = super(FeedbooksOPDSImporter, cls)._detail_for_elementtree_entry(
            parser, entry_tag, feed_url, do_get=do_get
        )
        rights_uri = cls.rights_uri_from_entry_tag(entry_tag)
        circulation = detail.setdefault('circulation', {})
        circulation['default_rights_uri'] =rights_uri
        return detail

    @classmethod
    def make_link_data(cls, rel, href=None, media_type=None, rights_uri=None,
                       content=None):
        """Turn basic link information into a LinkData object.

        FeedBooks puts open-access content behind generic
        'acquisition' links. We want to treat the EPUBs as open-access
        links and (at the request of FeedBooks) ignore the other
        formats.
        """
        if rel==Hyperlink.GENERIC_OPDS_ACQUISITION:
            if (media_type
                and media_type.startswith(Representation.EPUB_MEDIA_TYPE)):
                # Treat this generic acquisition link as an
                # open-access link.
                rel = Hyperlink.OPEN_ACCESS_DOWNLOAD
            else:
                # Feedbooks requests that we not mirror books in this format.
                # Act as if there was no link.
                return None

        return super(FeedbooksOPDSImporter, cls).make_link_data(
            rel, href, media_type, rights_uri, content
        )

    def improve_description(self, id, metadata):
        """Improve the description associated with a book,
        if possible.

        This involves fetching an alternate OPDS entry that might
        contain more detailed descriptions than those available in the
        main feed.
        """
        alternate_links = []
        existing_descriptions = []
        everything_except_descriptions = []
        for x in metadata.links:
            if (x.rel == Hyperlink.ALTERNATE and x.href
                and x.media_type == OPDSFeed.ENTRY_TYPE):
                alternate_links.append(x)
            if x.rel == Hyperlink.DESCRIPTION:
                existing_descriptions.append((x.media_type, x.content))
            else:
                everything_except_descriptions.append(x)

        better_descriptions = []
        for alternate_link in alternate_links:
            # There should only be one alternate link, but we'll keep
            # processing them until we get a good description.

            # Fetch the alternate entry.
            representation, is_new = Representation.get(
                self._db, alternate_link.href, max_age=self.THIRTY_DAYS,
                do_get=self.http_get
            )

            if representation.status_code != 200:
                continue

            # Parse the alternate entry with feedparser and run it through
            # data_detail_for_feedparser_entry().
            parsed = feedparser.parse(representation.content)
            if len(parsed['entries']) != 1:
                # This is supposed to be a single entry, and it's not.
                continue
            [entry] = parsed['entries']
            data_source = self.data_source
            detail_id, new_detail, failure = self.data_detail_for_feedparser_entry(
                entry, data_source
            )
            if failure:
                # There was a problem parsing the entry.
                self.log.error(failure.exception)
                continue

            # TODO: Ideally we could verify that detail_id == id, but
            # right now they are always different -- one is an HTTPS
            # URI and one is an HTTP URI. So we omit this step and
            # assume the documents at both ends of the 'alternate'
            # link identify the same resource.

            # Find any descriptions present in the alternate view which
            # are not present in the original.
            new_descriptions = [
                x for x in new_detail['links']
                if x.rel == Hyperlink.DESCRIPTION
                and (x.media_type, x.content) not in existing_descriptions
            ]

            if new_descriptions:
                # Replace old descriptions with new descriptions.
                metadata.links = (
                    everything_except_descriptions + new_descriptions
                )
                break

        return metadata

    def replace_css(self, representation):
        """This function will replace the content of every CSS file listed in an epub's
        manifest with the value in self.new_css. The rest of the file is not changed.
        """
        if not (representation.media_type == Representation.EPUB_MEDIA_TYPE and representation.content):
            return

        if not self.new_css:
            # There is no CSS to replace. Do nothing.
            return

        new_zip_content = StringIO()
        with EpubAccessor.open_epub(representation.url, content=representation.content) as (zip_file, package_path):
            try:
                manifest_element = EpubAccessor.get_element_from_package(
                    zip_file, package_path, 'manifest'
                )
            except ValueError as e:
                # Invalid EPUB
                self.log.warning("%s: %s" % (representation.url, str(e)))
                return

            css_paths = []
            for child in manifest_element:
                if child.tag == ("{%s}item" % EpubAccessor.IDPF_NAMESPACE):
                    if child.get('media-type') == "text/css":
                        href = package_path.replace(os.path.basename(package_path), child.get("href"))
                        css_paths.append(href)

            with ZipFile(new_zip_content, "w") as new_zip:
                for item in zip_file.infolist():
                    if item.filename not in css_paths:
                        new_zip.writestr(item, zip_file.read(item.filename))
                    else:
                        new_zip.writestr(item, self.new_css)

        representation.content = new_zip_content.getvalue()


class RehostingPolicy(object):
    """Determining the precise copyright status of the underlying text
    is not directly useful, because Feedbooks has made derivative
    works and relicensed under CC-BY-NC. So that's going to be the
    license: CC-BY-NC.

    Except it's not that simple. There are two complications.

    1. Feedbooks is located in France, and the NYPL/DPLA content
    servers are hosted in the US. We can't host a CC-BY-NC book if
    it's derived from a work that's still under US copyright. We must
    decide whether or not to accept a book in the first place based on
    the copyright status of the underlying text.

    2. Some CC licenses are more restrictive (on the creators of
    derivative works) than CC-BY-NC. Feedbooks has no authority to
    relicense these books, so the old licenses need to be preserved.

    This class encapsulates the logic necessary to make this decision.

    """

    PUBLIC_DOMAIN_CUTOFF = 1923

    # These are the licenses that need to be preserved.
    RIGHTS_DICT = {
        "Attribution Share Alike (cc by-sa)" : RightsStatus.CC_BY_SA,
        "Attribution Non-Commercial No Derivatives (cc by-nc-nd)" : RightsStatus.CC_BY_NC_ND,
        "Attribution Non-Commercial Share Alike (cc by-nc-sa)" : RightsStatus.CC_BY_NC_SA,
    }

    # Feedbooks rights statuses indicating books that can be rehosted
    # in the US.
    CAN_REHOST_IN_US = set([
        "This work was published before 1923 and is in the public domain in the USA only.",
        "This work is available for countries where copyright is Life+70 and in the USA.",
        'This work is available for countries where copyright is Life+50 or in the USA (published before 1923).',
        "Attribution (cc by)",
        "Attribution Non-Commercial (cc by-nc)",

        "Attribution Share Alike (cc by-sa)",
        "Attribution Non-Commercial No Derivatives (cc by-nc-nd)",
        "Attribution Non-Commercial Share Alike (cc by-nc-sa)",
    ])

    RIGHTS_UNKNOWN = "Please read the legal notice included in this e-book and/or check the copyright status in your country."

    # These websites are hosted in the US and specialize in
    # open-access content. We will accept all FeedBooks titles taken
    # from these sites, even post-1923 titles.
    US_SITES = set([
        "archive.org",
        "craphound.com",
        "en.wikipedia.org",
        "en.wikisource.org",
        "futurismic.com",
        "gutenberg.org",
        "project gutenberg",
        "shakespeare.mit.edu",
    ])

    @classmethod
    def rights_uri(cls, rights, source, publication_year):
        if publication_year and isinstance(publication_year, str):
            publication_year = int(publication_year)

        can_rehost = cls.can_rehost_us(rights, source, publication_year)
        if can_rehost is False:
            # We believe this book is still under copyright in the US
            # and we should not rehost it.
            return RightsStatus.IN_COPYRIGHT

        if can_rehost is None:
            # We don't have enough information to know whether the book
            # is under copyright in the US. We should not host it.
            return RightsStatus.UNKNOWN

        if rights in cls.RIGHTS_DICT:
            # The CC license of the underlying text means it cannot be
            # relicensed CC-BY-NC.
            return cls.RIGHTS_DICT[rights]

        # The default license as per our agreement with FeedBooks.
        return RightsStatus.CC_BY_NC

    @classmethod
    def can_rehost_us(cls, rights, source, publication_year):
        """Can we rehost this book on a US server?

        :param rights: What FeedBooks says about the public domain status
            of the book.

        :param source: Where FeedBooks got the book.

        :param publication_year: When the text was originally published.

        :return: True if we can rehost in the US, False if we can't,
            None if we're not sure. The distinction between False and None
            is only useful when making lists of books that need to have
            their rights status manually investigated.
        """
        if publication_year and publication_year < cls.PUBLIC_DOMAIN_CUTOFF:
            # We will rehost anything published prior to 1923, no
            # matter where it came from.
            return True

        if rights in cls.CAN_REHOST_IN_US:
            # This book's FeedBooks rights statement explicitly marks
            # it as one that can be rehosted in the US.
            return True

        # The rights statement isn't especially helpful, but maybe we
        # can make a determination based on where Feedbooks got the
        # book from.
        source = (source or "").lower()

        if any(site in source for site in cls.US_SITES):
            # This book originally came from a US-hosted site that
            # specializes in open-access books, so we must be able
            # to rehost it.
            return True

        if source in ('wikisource', 'gutenberg'):
            # Presumably en.wikisource and Project Gutenberg US.  We
            # special case these to avoid confusing the US versions of
            # these sites with other countries'.
            return True

        # And we special-case this one to avoid confusing Australian
        # Project Gutenberg with US Project Gutenberg.
        if ('gutenberg.net' in source and not 'gutenberg.net.au' in source):
            return True

        # Unless one of the above conditions is met, we must assume
        # the book cannot be rehosted in the US.
        if rights == cls.RIGHTS_UNKNOWN:
            # To be on the safe side we're not going to host this
            # book, but we actually don't know that it's unhostable.
            return None

        # In this case we're pretty sure. The rights status indicates
        # some kind of general incompatible restriction (such as
        # Life+70) and it's not a pre-1923 book.
        return False

class FeedbooksImportMonitor(OPDSImportMonitor):
    """The same as OPDSImportMonitor, but uses FeedbooksOPDSImporter
    instead.
    """

    PROTOCOL = ExternalIntegration.FEEDBOOKS

    def data_source(self, collection):
        """The data source for all Feedbooks collections is Feedbooks."""
        return ExternalIntegration.FEEDBOOKS

    def opds_url(self, collection):
        """Returns the OPDS import URL for the given collection.

        This is the base URL plus the language setting.
        """
        language = collection.external_account_id or 'en'
        return FeedbooksOPDSImporter.BASE_OPDS_URL % dict(language=language)
