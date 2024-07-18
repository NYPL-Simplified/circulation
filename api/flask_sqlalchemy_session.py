"""
https://github.com/dtheodor/flask-sqlalchemy-session is archived

Implements fixes to make it work with modern Werkzeug
https://github.com/dtheodor/flask-sqlalchemy-session/pull/17
https://github.com/dtheodor/flask-sqlalchemy-session/pull/15
"""

from flask import current_app
from sqlalchemy.orm import scoped_session
from werkzeug.local import LocalProxy


def _get_session():
    app = current_app._get_current_object()
    if not hasattr(app, "scoped_session"):
        raise AttributeError(
            "{} has no 'scoped_session' attribute. You need to initialize it "
            "with a flask_scoped_session.".format(app)
        )
    return app.scoped_session


current_session = LocalProxy(_get_session)
"""Provides the current SQL Alchemy session within a request.

Will raise an exception if no :data:`~flask.current_app` is available or it has
not been initialized with a :class:`flask_scoped_session`
"""


class flask_scoped_session(scoped_session):
    """A :class:`~sqlalchemy.orm.scoping.scoped_session` whose scope is set to
    the Flask application context.
    """

    def __init__(self, session_factory, app=None):
        """
        :param session_factory: A callable that returns a
            :class:`~sqlalchemy.orm.session.Session`
        :param app: a :class:`~flask.Flask` application
        """
        try:
            from greenlet import getcurrent as scopefunc
        except ImportError:
            from threading import get_ident as scopefunc
        super().__init__(session_factory, scopefunc=scopefunc)
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Setup scoped session creation and teardown for the passed ``app``.

        :param app: a :class:`~flask.Flask` application
        """
        app.scoped_session = self

        @app.teardown_appcontext
        def remove_scoped_session(*args, **kwargs):
            app.scoped_session.remove()
