from apispec import BasePlugin
import flask


class CSRFPlugin(BasePlugin):
    def parameter_helper(self, parameter, **kwargs):
        if parameter['name'] == 'X-CSRF-Token':
            csrf_token = flask.request.cookies.get("csrf_token")
            parameter['schema']['default'] = csrf_token

        return parameter
