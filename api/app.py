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
from flask_babel import Babel


app = Flask(__name__)
app._db = None
app.config['BABEL_DEFAULT_LOCALE'] = LanguageCodes.three_to_two[Configuration.localization_languages()[0]]
app.config['BABEL_TRANSLATION_DIRECTORIES'] = "../translations"
babel = Babel(app)

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
    logging.getLogger().info("Application debug mode==%r" % app.debug)

import routes
import admin.routes

def run(url=None):
    base_url = url or u'http://localhost:6500/'
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


