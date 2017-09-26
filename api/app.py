from nose.tools import set_trace
import os
import logging
import urlparse

import flask
from flask import (
    Flask, 
    Response,
    redirect,
)
from flask_sqlalchemy_session import flask_scoped_session
from sqlalchemy.orm import sessionmaker
from config import Configuration
from core.model import (
    ConfigurationSetting,
    SessionManager,
)
from core.log import LogConfiguration
from core.util import LanguageCodes
from flask.ext.babel import Babel

app = Flask(__name__)

testing = 'TESTING' in os.environ
db_url = Configuration.database_url(testing)
# Initialize a new database session unless we were told not to
# (e.g. because a script already initialized it).
autoinitialize = os.environ.get('AUTOINITIALIZE') != 'False'
if autoinitialize:
    SessionManager.initialize(db_url)
session_factory = SessionManager.sessionmaker(db_url)
_db = flask_scoped_session(session_factory, app)
if autoinitialize:
    SessionManager.initialize_data(_db)

app.config['BABEL_DEFAULT_LOCALE'] = LanguageCodes.three_to_two[Configuration.localization_languages()[0]]
app.config['BABEL_TRANSLATION_DIRECTORIES'] = "../translations"
babel = Babel(app)

import routes
import admin.routes

log_level = LogConfiguration.initialize(_db, testing=testing)
debug = log_level == 'DEBUG'

logging.getLogger().info("Application debug mode==%r" % debug)
app.config['DEBUG'] = debug
app.debug = debug

def run(url=None):
    base_url = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
    base_url = url or base_url.value or u'http://localhost:6500/'
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(base_url)
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
    app.run(debug=debug, host=host, port=port, threaded=True)


