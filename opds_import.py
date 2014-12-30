from monitor import Monitor

from lxml import builder, etree


class BaseOPDSImporter(object):
    """Capable of importing editions from an OPDS feed.

    This importer should be used when a circulation server
    communicates with a content server. It ignores author and subject
    information, under the assumption that it will get better author
    and subject information from the metadata wrangler.
    """
   
    def __init__(self, _db, feed):
        self._db = _db
        self.data_source = data_source
        self.raw_feed = unicode(feed)
        self.feedparser_parsed = feedparser.parse(self.raw_feed)

    def import_from_feed(self):
        imported = []
        for entry in self.feedparser_parsed['entries']:
            edition, opds_id = self.import_from_feedparser_entry(entry)
            imported.append(editions)

        return imported

    def import_from_feedparser_entry(self, entry):
        identifier = Identifier.parse_urn(self._db, entry.get('id'))
        data_source = DataSource.license_source_for(self._db, identifier)

        title = entry.get('title', None)
        updated = entry.get('updated_parsed', None)
        publisher = entry.get('dcterms_publisher', None)
        language = entry.get('dcterms_language', None)
        summary = entry.get('summary', '')
        pwid = entry.get('simplified_pwid', None)

        title_detail = entry.get('title_detail', None)
        summary_detail = entry.get('summary_detail', None)

        links = entry.get('links', [])

        # Make sure there's an open-access link. Otherwise we 
        # can't create a LicensePool.
        set_trace()

        # Create or retrieve a LicensePool for this book.
        license_pool, pool_was_new = LicensePool.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier)
        if pool_was_new:
            pool.open_access = True
        
        # Create or retrieve an Edition for this book.
        edition, edition_was_new = Edition.for_foreign_id(
            self._db, data_source, identifier.type, identifier.identifier)
        edition.title = title

        # Associate the open access links and the covers with the
        # identifier.
        

    def process_tags(self, tags):
        """Turn a list of tags into a list of Subject objects."""
        

class DetailedOPDSImporter(BaseOPDSImporter):

    """An OPDS importer that imports authors as contributors and
    tags as subjects.

    This should be used by circulation managers when talking to the
    metadata wrangler, and by the metadata wrangler when talking to
    content servers.
    """
    def import_from_feed(self, feed):
        lxml_parsed = etree.parse(feed)
        authors_by_id = self.authors_by_id(lxml_parsed)
        subjects = self.process_tags(entry.get('tags', []))


class OPDSImportMonitor(Monitor):
    """Periodically monitor an OPDS archive feed and import every edition
    it mentions.
    """
    
    def __init__(self, feed_url, interval_seconds=3600):
        self.feed_url = feed_url
        super(OPDSImportMonitor, self).__init__(
            "OPDS Import %s" % feed_url, interval_seconds)

    def run_once(self, _db, start, cutoff):
        importer = OPDSImporter(_db)
        next_link = self.feed_url
        while next_link:
            imported, parsed = self.process_one_page(importer, next_link)
            if len(imported) == 0:
                # We did not see a single book on this page we haven't
                # already seen. There's no need to keep going.
                break
            # TODO: get the proper next link.
            set_trace()
            next_link = True

    def process_one_page(self, importer, url):
        response = requests.get(self.feed_url)
        return importer.import_from_feed(response.content)
