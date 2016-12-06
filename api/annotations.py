from nose.tools import set_trace
from pyld import jsonld
import json
from datetime import datetime

from core.model import (
    Annotation,
    Identifier,
    get_one_or_create,
)

from core.app_server import (
    url_for,
)

from problem_details import *

class AnnotationWriter(object):

    CONTENT_TYPE = 'application/ld+json; profile="http://www.w3.org/ns/anno.jsonld"'

    JSONLD_CONTEXT = "http://www.w3.org/ns/anno.jsonld"
    LDP_CONTEXT = "http://www.w3.org/ns/ldp.jsonld"

    @classmethod
    def annotations_for(cls, patron, identifier=None):
        annotations = [annotation for annotation in patron.annotations if annotation.active]
        if identifier:
            annotations = [annotation for annotation in annotations if annotation.identifier == identifier]
        return annotations

    @classmethod
    def annotation_container_for(cls, patron, identifier=None):
        if identifier:
            url = url_for('annotations_for_work',
                          identifier_type=identifier.type,
                          identifier=identifier.identifier,
                          _external=True)
        else:
            url = url_for("annotations", _external=True)
        annotations = cls.annotations_for(patron, identifier=identifier)

        latest_timestamp = None
        if len(annotations) > 0:
            # patron.annotations is already sorted by timestamp, so the first
            # annotation is the most recent.
            latest_timestamp = annotations[0].timestamp

        container = dict()
        container["@context"] = [cls.JSONLD_CONTEXT, cls.LDP_CONTEXT]
        container["id"] = url
        container["type"] = ["BasicContainer", "AnnotationCollection"]
        container["total"] = len(annotations)
        container["first"] = cls.annotation_page_for(patron, identifier=identifier, with_context=False)
        return container, latest_timestamp
        

    @classmethod
    def annotation_page_for(cls, patron, identifier=None, with_context=True):
        if identifier:
            url = url_for('annotations_for_work',
                          identifier_type=identifier.type,
                          identifier=identifier.identifier,
                          _external=True)
        else:
            url = url_for("annotations", _external=True)
        annotations = cls.annotations_for(patron, identifier=identifier)
        details = [cls.detail(annotation, with_context=with_context) for annotation in annotations]

        page = dict()
        if with_context:
            page["@context"] = cls.JSONLD_CONTEXT
        page["id"] = url
        page["type"] = "AnnotationPage"
        page["items"] = details
        return page

    @classmethod
    def detail(cls, annotation, with_context=True):
        item = dict()
        if with_context:
            item["@context"] = cls.JSONLD_CONTEXT
        item["id"] = url_for("annotation_detail", annotation_id=annotation.id, _external=True)
        item["type"] = "Annotation"
        item["motivation"] = annotation.motivation
        item["body"] = annotation.content
        if annotation.target:
            target = json.loads(annotation.target)
            compacted = jsonld.compact(target, cls.JSONLD_CONTEXT)
            del compacted["@context"]
            item["target"] = compacted
        if annotation.content:
            body = json.loads(annotation.content)
            compacted = jsonld.compact(body, cls.JSONLD_CONTEXT)
            del compacted["@context"]
            item["body"] = compacted

        return item

class AnnotationParser(object):

    @classmethod
    def parse(cls, _db, data, patron):
        try:
            data = json.loads(data)
            data = jsonld.expand(data)
        except ValueError, e:
            return INVALID_ANNOTATION_FORMAT

        if not data or not len(data) == 1:
            return INVALID_ANNOTATION_TARGET
        data = data[0]

        target = data.get("http://www.w3.org/ns/oa#hasTarget")
        if not target or not len(target) == 1:
            return INVALID_ANNOTATION_TARGET
        target = target[0]

        source = target.get("http://www.w3.org/ns/oa#hasSource")

        if not source or not len(source) == 1:
            return INVALID_ANNOTATION_TARGET
        source = source[0].get('@id')

        identifier, ignore = Identifier.parse_urn(_db, source)
        
        motivation = data.get("http://www.w3.org/ns/oa#motivatedBy")
        if not motivation or not len(motivation) == 1:
            return INVALID_ANNOTATION_MOTIVATION
        motivation = motivation[0].get('@id')
        if motivation not in Annotation.MOTIVATIONS:
            return INVALID_ANNOTATION_MOTIVATION

        loans = patron.loans
        loan_identifiers = [loan.license_pool.identifier for loan in loans]
        if identifier not in loan_identifiers:
            return INVALID_ANNOTATION_TARGET

        content = data.get("http://www.w3.org/ns/oa#hasBody")
        if content and len(content) == 1:
            content = content[0]
        else:
            content = None

        annotation, is_new = get_one_or_create(
            _db, Annotation,
            patron=patron,
            identifier=identifier,
            motivation=motivation,
        )

        annotation.target = json.dumps(target)
        if content:
            annotation.content = json.dumps(content)
        annotation.active = True
        annotation.timestamp = datetime.now()

        return annotation
