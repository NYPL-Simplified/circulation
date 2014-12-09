"""Simple helper library for generating problem detail documents."""
import json as j

JSON_MEDIA_TYPE = "application/problem+json"

def json(type, title, status, detail=None, instance=None):
    d = dict(type=type, title=title, status=status)
    if detail:
        d['detail'] = detail
    if instance:
        d['instance'] = instance
    return j.dumps(d)
