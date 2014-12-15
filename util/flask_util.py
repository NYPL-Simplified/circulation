"""Utilities for Flask applications."""
import flask
from flask import Response

from . import (
    problem_detail,
    languages_from_accept
)

def problem(type, title, status, detail=None, instance=None, headers={}):
    """Create a Response that includes a Problem Detail Document."""
    data = problem_detail.json(type, title, status, detail, instance)
    final_headers = { "Content-Type" : problem_detail.JSON_MEDIA_TYPE }
    final_headers.update(headers)
    return Response(data, status, headers)
    
def languages_for_request():
    return languages_from_accept(flask.request.accept_languages)
