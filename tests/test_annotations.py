from nose.tools import (
    eq_,
    set_trace,
)
import json
import datetime

from . import DatabaseTest

from core.model import (
    Annotation,
    create,
)

from api.annotations import (
    AnnotationWriter,
    AnnotationParser,
)
from api.problem_details import *

class AnnotationWriterTest(DatabaseTest):
    def test_annotation_container_for(self):
        patron = self._patron()

        container, timestamp = AnnotationWriter.annotation_container_for(patron)
        eq_(set([AnnotationWriter.JSONLD_CONTEXT, AnnotationWriter.LDP_CONTEXT]),
            set(container['@context']))
        assert "annotations" in container["id"]
        eq_(set(["BasicContainer", "AnnotationCollection"]), set(container["type"]))
        eq_(0, container["total"])

        first_page = container["first"]
        eq_("AnnotationPage", first_page["type"])

        # The page doesn't have a context, since it's in the container.
        eq_(None, first_page.get('@context'))

        # The patron doesn't have any annotations yet.
        eq_(0, container['total'])

        # There's no timestamp since the container is empty.
        eq_(None, timestamp)

        # Now, add an annotation.
        identifier = self._identifier()
        annotation = create(
            self._db, Annotation,
            patron_id=patron.id,
            identifier_id=identifier.id,
            motivation=Annotation.IDLING,
        )
        annotation.timestamp = datetime.datetime.now()
        
        container, timestamp = AnnotationWriter.annotation_container_for(patron)

        # The context, type, and id stay the same.
        eq_(set([AnnotationWriter.JSONLD_CONTEXT, AnnotationWriter.LDP_CONTEXT]),
            set(container['@context']))
        assert "annotations" in container["id"]
        eq_(set(["BasicContainer", "AnnotationCollection"]), set(container["type"]))

        # But now there is one item.
        eq_(1, container['total'])

        first_page = container["first"]

        eq_(1, len(first_page['items']))

        # The item doesn't have a context, since it's in the container.
        first_item = first_page['items'][0]
        eq_(None, first_item['@context'])

        # The timestamp is the annotation's timestamp.
        eq_(annotation.timestamp, timestamp)

        # If the annotation is deleted, the container will be empty again.
        annotation.active = False

        container, timestamp = AnnotationWriter.annotation_container_for(patron)
        eq_(0, container['total'])
        eq_(None, timestamp)
        
    def test_annotation_page_for(self):
        patron = self._patron()
        page = AnnotationWriter.annotation_page_for(patron)

        # The patron doesn't have any annotations, so the page is empty.
        eq_(AnnotationWriter.JSONLD_CONTEXT, page['@context'])
        assert 'annotations' in page['id']
        eq_('AnnotationPage', page['type'])
        eq_(0, len(page['items']))

        # If we add an annotation, the page will have an item.
        identifier = self._identifier()
        annotation = create(
            self._db, Annotation,
            patron_id=patron.id,
            identifier_id=identifier.id,
            motivation=Annotation.IDLING,
        )

        page = AnnotationWriter.annotation_page_for(patron)

        eq_(1, len(page['items']))

        # But if the annotation is deleted, the page will be empty again.
        annotation.active = False

        page = AnnotationWriter.annotation_page_for(patron)

        eq_(0, len(page['items']))

    def test_detail(self):
        patron = self._patron()
        identifier = self._identifier()
        annotation = create(
            self._db, Annotation,
            patron_id=patron.id,
            identifier_id=identifier.id,
            motivation=Annotation.IDLING,
        )

        detail = AnnotationWriter.detail(annotation)
        assert "annotations/%i" % annotation.id in detail["id"]
        eq_("Annotation", detail['type'])
        eq_(Annotation.IDLING, detail['motivation'])


class TestAnnotationParser(DatabaseTest):
    def setup(self):
        super(TestAnnotationParser, self).setup()
        self.pool = self._licensepool(None)
        self.identifier = self.pool.identifier

    def _sample_jsonld(self):
        data = dict()
        data["@context"] = [AnnotationWriter.JSONLD_CONTEXT, 
                            {'ls': Annotation.LS_NAMESPACE}]
        data["type"] = "Annotation"
        data["motivation"] = Annotation.IDLING.replace(Annotation.LS_NAMESPACE, 'ls:')
        data["body"] = {
            "type": "TextualBody",
            "bodyValue": "A good description of the topic that bears further investigation",
            "purpose": "describing"
        }
        data["target"] = {
            "source": self.identifier.urn,
            "selector": {
                "type": "oa:FragmentSelector",
                "value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)"
            }
        }
        return data

    def test_parse_invalid_json(self):
        annotation = AnnotationParser.parse(self._db, "not json", self.default_patron)
        eq_(INVALID_ANNOTATION_FORMAT, annotation)

    def test_parse_expanded_jsonld(self):
        self.pool.loan_to(self.default_patron)

        data = dict()
        data['@type'] = ["http://www.w3.org/ns/oa#Annotation"]
        data["http://www.w3.org/ns/oa#motivatedBy"] = [{
            "@id": Annotation.IDLING
        }]
        data["http://www.w3.org/ns/oa#hasBody"] = [{
            "@type" : ["http://www.w3.org/ns/oa#TextualBody"],
            "http://www.w3.org/ns/oa#bodyValue": [{
                "@value": "A good description of the topic that bears further investigation"
            }],
            "http://www.w3.org/ns/oa#hasPurpose": [{
                "@id": "http://www.w3.org/ns/oa#describing"
            }]
        }]
        data["http://www.w3.org/ns/oa#hasTarget"] = [{
            "http://www.w3.org/ns/oa#hasSource": [{
                "@id": self.identifier.urn
            }],
            "http://www.w3.org/ns/oa#hasSelector": [{
                "@type": ["http://www.w3.org/ns/oa#FragmentSelector"],
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": [{
                    "@value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)"
                }]
            }]
        }]

        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)
        eq_(self.default_patron.id, annotation.patron_id)
        eq_(self.identifier.id, annotation.identifier_id)
        eq_(Annotation.IDLING, annotation.motivation)
        eq_(True, annotation.active)

    def test_parse_compacted_jsonld(self):
        self.pool.loan_to(self.default_patron)

        data = dict()
        data["@type"] = "http://www.w3.org/ns/oa#Annotation"
        data["http://www.w3.org/ns/oa#motivatedBy"] = {
            "@id": Annotation.IDLING
        }
        data["http://www.w3.org/ns/oa#hasBody"] = {
            "@type": "http://www.w3.org/ns/oa#TextualBody",
            "http://www.w3.org/ns/oa#bodyValue": "A good description of the topic that bears further investigation",
            "http://www.w3.org/ns/oa#hasPurpose": {
                "@id": "http://www.w3.org/ns/oa#describing"
            }
        }
        data["http://www.w3.org/ns/oa#hasTarget"] = {
            "http://www.w3.org/ns/oa#hasSource": {
                "@id": self.identifier.urn
            },
            "http://www.w3.org/ns/oa#hasSelector": {
                "@type": "http://www.w3.org/ns/oa#FragmentSelector",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": "epubcfi(/6/4[chap01ref]!/4[body01]/10[para05]/3:10)"
            }
        }

        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)
        eq_(self.default_patron.id, annotation.patron_id)
        eq_(self.identifier.id, annotation.identifier_id)
        eq_(Annotation.IDLING, annotation.motivation)
        eq_(True, annotation.active)

    def test_parse_jsonld_with_context(self):
        self.pool.loan_to(self.default_patron)

        data = self._sample_jsonld()
        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)

        eq_(self.default_patron.id, annotation.patron_id)
        eq_(self.identifier.id, annotation.identifier_id)
        eq_(Annotation.IDLING, annotation.motivation)
        eq_(True, annotation.active)

    def test_parse_jsonld_with_invalid_motivation(self):
        self.pool.loan_to(self.default_patron)

        data = self._sample_jsonld()
        data["motivation"] = "bookmarking"
        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)

        eq_(INVALID_ANNOTATION_MOTIVATION, annotation)

    def test_parse_jsonld_with_no_loan(self):
        data = self._sample_jsonld()
        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)

        eq_(INVALID_ANNOTATION_TARGET, annotation)

    def test_parse_jsonld_with_no_target(self):
        data = self._sample_jsonld()
        del data['target']
        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)

        eq_(INVALID_ANNOTATION_TARGET, annotation)

    def test_parse_updates_existing_annotation(self):
        self.pool.loan_to(self.default_patron)

        original_annotation, ignore = create(
            self._db, Annotation,
            patron_id=self.default_patron.id,
            identifier_id=self.identifier.id,
            motivation=Annotation.IDLING,
        )
        original_annotation.active = False
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        original_annotation.timestamp = yesterday

        data = self._sample_jsonld()
        data = json.dumps(data)

        annotation = AnnotationParser.parse(self._db, data, self.default_patron)

        eq_(original_annotation, annotation)
        eq_(True, annotation.active)
        assert annotation.timestamp > yesterday
