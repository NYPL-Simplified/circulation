from nose.tools import (
    eq_,
)
from util.web_publication_manifest import (
    JSONable,
    Manifest,
    TimelinePart,
    AudiobookManifest,
)

class TestJSONable(object):

    class Mock(JSONable):
        @property
        def as_dict(self):
            return dict(value=1)

    eq_(u'{"value": 1}', unicode(Mock()))
