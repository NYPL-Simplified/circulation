"""Utilities for Flask applications."""
import flask
from flask import Response

from . import (
    problem_detail,
    languages_from_accept
)

def problem_raw(type, status, title, detail=None, instance=None, headers={}):
    data = problem_detail.json(type, status, title, detail, instance)
    final_headers = { "Content-Type" : problem_detail.JSON_MEDIA_TYPE }
    final_headers.update(headers)
    return status, final_headers, data

def problem(type, status, title, detail=None, instance=None, headers={}):
    """Create a Response that includes a Problem Detail Document."""
    status, headers, data = problem_raw(
        type, status, title, detail, instance, headers)
    return Response(data, status, headers)

def languages_for_request():
    return languages_from_accept(flask.request.accept_languages)
