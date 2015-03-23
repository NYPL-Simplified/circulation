from nose.tools import set_trace
from cStringIO import StringIO
import csv
import os
import sys
from datetime import timedelta
from sqlalchemy import or_
from core.model import (
    Contribution,
    CustomList,
    DataSource,
    Edition,
    Hyperlink,
    Identifier,
    Representation,
)
from core.scripts import Script
from core.opds_import import (
    SimplifiedOPDSLookup,
    DetailedOPDSImporter,
    BaseOPDSImporter,
)
from core.opds import OPDSFeed
from core.external_list import CustomListFromCSV

class CreateWorksForIdentifiersScript(Script):

    """Do the bare minimum to associate each Identifier with an Edition
    with title and author, so that we can calculate a permanent work
    ID.
    """
    to_check = [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID]
    BATCH_SIZE = 100

    def __init__(self, metadata_web_app_url=None):
        self.metadata_url = (metadata_web_app_url
                             or os.environ['METADATA_WEB_APP_URL'])
        self.lookup = SimplifiedOPDSLookup(self.metadata_url)

    def run(self):

        # We will try to fill in Editions that are missing
        # title/author and as such have no permanent work ID.
        #
        # We will also try to create Editions for Identifiers that
        # have no Edition.

        either_title_or_author_missing = or_(
            Edition.title == None,
            Edition.sort_author == None,
        )
        edition_missing_title_or_author = self._db.query(Identifier).join(
            Identifier.primarily_identifies).filter(
                either_title_or_author_missing)

        no_edition = self._db.query(Identifier).filter(
            Identifier.primarily_identifies==None).filter(
                Identifier.type.in_(self.to_check))

        for q in (edition_missing_title_or_author, no_edition):
            batch = []
            print "%d total." % q.count()
            for i in q:
                batch.append(i)
                if len(batch) >= self.BATCH_SIZE:
                    self.process_batch(batch)
                    batch = []

    def process_batch(self, batch):
        print "%d batch" % len(batch)
        response = self.lookup.lookup(batch)
        print "Response!"

        if response.status_code != 200:
            raise Exception(response.text)
            
        content_type = response.headers['content-type']
        if content_type != OPDSFeed.ACQUISITION_FEED_TYPE:
            raise Exception("Wrong media type: %s" % content_type)

        importer = DetailedOPDSImporter(
            self._db, response.text,
            [Hyperlink.DESCRIPTION, Hyperlink.IMAGE])
        imported, messages_by_id = importer.import_from_feed()
        print "%d successes, %d failures." % (len(imported), len(messages_by_id))
        self._db.commit()

class MetadataCalculationScript(Script):

    """Force calculate_presentation() to be called on some set of Editions.

    This assumes that the metadata is in already in the database and
    will fall into place if we just call
    Edition.calculate_presentation() and Edition.calculate_work() and
    Work.calculate_presentation().

    Most of these will be data repair scripts that do not need to be run
    regularly.

    """

    def q(self):
        raise NotImplementedError()

    def run(self):
        q = self.q()
        print "Attempting to repair %d" % q.count()

        success = 0
        failure = 0
        also_created_work = 0

        def checkpoint():
            self._db.commit()
            print "%d successes, %d failures, %d new works." % (
                success, failure, also_created_work)

        i = 0
        for edition in q:
            edition.calculate_presentation()
            if edition.sort_author:
                success += 1
                work, is_new = edition.license_pool.calculate_work()
                if work:
                    work.calculate_presentation()
                    if is_new:
                        also_created_work += 1
            else:
                failure += 1
            i += 1
            if not i % 1000:
                checkpoint()
        checkpoint()

class FillInAuthorScript(MetadataCalculationScript):
    """Fill in Edition.sort_author for Editions that have a list of
    Contributors, but no .sort_author.

    This is a data repair script that should not need to be run
    regularly.
    """

    def q(self):
        return self._db.query(Edition).join(
            Edition.contributions).join(Contribution.contributor).filter(
                Edition.sort_author==None)

class UpdateStaffPicksScript(Script):

    DEFAULT_URL_TEMPLATE = "https://docs.google.com/spreadsheets/d/%s/export?format=csv"

    def run(self):
        key = os.environ['STAFF_PICKS_GOOGLE_SPREADSHEET_KEY']
        if key.startswith('https://') or key.startswith('http://'):
            # It's a custom URL, not a Google spreadsheet key.
            # Leave it alone.
            pass
        else:
            url = self.DEFAULT_URL_TEMPLATE % key
        metadata_client = None
        representation, cached = Representation.get(
            self._db, url, do_get=Representation.browser_http_get,
            accept="text/csv", max_age=timedelta(days=1))
        if representation.status_code != 200:
            raise ValueError("Unexpected status code %s" % 
                             representation.status_code)
            return
        if not representation.media_type.startswith("text/csv"):
            raise ValueError("Unexpected media type %s" % 
                             representation.media_type)
            return
        importer = CustomListFromCSV(
            DataSource.LIBRARY_STAFF, CustomList.STAFF_PICKS_NAME)
        reader = csv.DictReader(StringIO(representation.content))
        writer = csv.writer(sys.stdout)
        importer.to_customlist(self._db, reader, writer)
        self._db.commit()


class ContentOPDSImporter(BaseOPDSImporter):

    # The content server is the canonical source for open-access
    # links, but not for anything else.
    OVERWRITE_RELS = [Hyperlink.OPEN_ACCESS]

    def __init__(self, _db, feed):
        super(ContentOPDSImporter, self).__init__(_db, self.OVERWRITE_RELS)
