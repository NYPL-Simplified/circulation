import os
from core.model import (
    Identifier,
)
from core.scripts import Script
from core.opds_import import (
    SimplifiedOPDSLookup,
    DetailedOPDSImporter,
)
from core.opds import OPDSFeed

class CreateWorksForIdentifiersScript(Script):

    """Do the bare minimum to associate each Identifier with an Edition
    with title and author, so that we can calculate a permanent work
    ID.
    """
    to_check = [Identifier.OVERDRIVE_ID, Identifier.THREEM_ID,
                Identifier.GUTENBERG_ID]
    BATCH_SIZE = 1000

    def __init__(self, metadata_web_app_url=None):
        self.metadata_url = (metadata_web_app_url
                             or os.environ['METADATA_WEB_APP_URL'])
        self.lookup = SimplifiedOPDSLookup(self.metadata_url)

    def run(self):
        q = self._db.query(Identifier).filter(
            Identifier.primarily_identifies==None).filter(
                Identifier.type.in_(self.to_check))
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

        importer = DetailedOPDSImporter(self._db, response.text)
        imported, messages_by_id = importer.import_from_feed()
        print "%d successes, %d failures." % (len(imported), len(messages_by_id))
        self._db.commit()
