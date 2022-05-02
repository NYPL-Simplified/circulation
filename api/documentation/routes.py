import inspect
from apispec.exceptions import APISpecError

from api.app import app
from api.openapi import generateSpecBase
from api import routes
from api.admin import routes as adminRoutes

import logging


@app.route('/documentation')
def generate_documentation():
    spec = generateSpecBase()
    for name, method in adminRoutes.__dict__.items():
        if inspect.isfunction(method):
            try:
                spec.path(view=method)
            except APISpecError:
                logging.debug(f'{name} unable to create view')
                pass

    return spec.to_dict()