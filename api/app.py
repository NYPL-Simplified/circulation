import os
from newrelic import agent
from newrelic.api.exceptions import ConfigurationError

if os.environ.get('NEW_RELIC_LICENSE_KEY', None):
    try:
        agent.initialize(
            config_file='/home/simplified/circulation/newrelic.ini',
            environment=os.environ.get('DEVELOPMENT_STAGE', 'local')
        )
    except ConfigurationError:
        # If we receive this error, the NR Agent was initialized in a script
        # and therefore we can safely skip this (some scripts use API calls)
        pass

import logging
import urllib.parse
from flask import (
    Flask,
    Response,
    redirect,
)
from flask_swagger_ui import get_swaggerui_blueprint
from flask_sqlalchemy_session import flask_scoped_session
from .config import Configuration
from core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    Library,
    SessionManager,
    create,
    get_one_or_create,
)
from core.external_search import ExternalSearchIndex
from core.log import LogConfiguration
from core.util import LanguageCodes
from flask_babel import Babel


app = Flask(__name__)
app._db = None
app.static_resources_dir = Configuration.static_resources_dir()
app.config['BABEL_DEFAULT_LOCALE'] = LanguageCodes.three_to_two[Configuration.localization_languages()[0]]
app.config['BABEL_TRANSLATION_DIRECTORIES'] = "../translations"
babel = Babel(app)

swaggerui_print = get_swaggerui_blueprint(
    '/apidocs', '/documentation'
)
app.register_blueprint(swaggerui_print)

@app.before_first_request
def initialize_database(autoinitialize=True):
    testing = 'TESTING' in os.environ

    db_url = Configuration.database_url()
    if autoinitialize:
        SessionManager.initialize(db_url)
    session_factory = SessionManager.sessionmaker(db_url)
    _db = flask_scoped_session(session_factory, app)
    app._db = _db

    log_level = LogConfiguration.initialize(_db, testing=testing)
    debug = log_level == 'DEBUG'
    app.config['DEBUG'] = debug
    app.debug = debug
    _db.commit()

    # If no global ElasticSearch integration exists in the database, and an ES URL
    # is provided in the environment, create an integration based on that.    
    es_integrations = _db.query(ExternalIntegration).filter(
        ExternalIntegration.protocol==ExternalIntegration.ELASTICSEARCH,
        ExternalIntegration.goal==ExternalIntegration.SEARCH_GOAL
    ).filter(Library.id==None).all()
    es_url_from_env = os.environ.get('SIMPLIFIED_ELASTICSEARCH_URL')

    if not es_integrations and not testing and es_url_from_env:
        (es_integration, _) = get_one_or_create(
            _db,
            ExternalIntegration,
            name="LocalDevElasticSearch",
            goal=ExternalIntegration.SEARCH_GOAL,
            protocol=ExternalIntegration.ELASTICSEARCH
        )
        es_integration.set_setting("url", es_url_from_env)
        es_integration.set_setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY,
                                   ExternalSearchIndex.DEFAULT_WORKS_INDEX_PREFIX)
        es_integration.set_setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY,
                                   ExternalSearchIndex.DEFAULT_TEST_SEARCH_TERM)
        _db.commit()

    logging.getLogger().info("Application debug mode==%r" % app.debug)

from . import routes
from .admin import routes
from .documentation import routes

def run(url=None):
    base_url = url or 'http://localhost:6500/'
    scheme, netloc, path, parameters, query, fragment = urllib.parse.urlparse(base_url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80

    # Required for subdomain support.
    app.config['SERVER_NAME'] = netloc

    debug = True

    # Workaround for a "Resource temporarily unavailable" error when
    # running in debug mode with the global socket timeout set by isbnlib
    if debug:
        import socket
        socket.setdefaulttimeout(None)

    logging.info("Starting app on %s:%s", host, port)
    sslContext = 'adhoc' if scheme == 'https' else None
    app.run(debug=debug, host=host, port=port, threaded=True, ssl_context=sslContext)