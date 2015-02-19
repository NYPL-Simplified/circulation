import datetime

class MirrorUploader(object):

    """Handles the job of uploading a representation's content to 
    a mirror that we control.
    """

    def do_upload(self, representation):
        raise NotImplementedError()        

    def mirror_one(self, representation):
        """Mirror a single Representation."""
        now = datetime.datetime.utcnow()
        exception = self.do_upload(representation)
        representation.mirror_exception = exception
        if exception:
            representation.mirrored_at = None
        else:
            representation.mirrored_at = now

    def mirror_batch(self, representations):
        """Mirror a batch of Representations at once."""

        for representation in representations:
            self.mirror_one(representation)
