from cStringIO import StringIO
from nose.tools import set_trace
import os
import urlparse
import csv

import flask
from flask import Flask, request, url_for, redirect, Response

from core.external_list import (
    CustomListFromCSV,
    CSVFormatError,
)
from core.opds_import import (
    SimplifiedOPDSLookup,
)
from core.model import (
    DataSource,
    production_session,
)

class Conf:
    db = None
    metadata_client = None

    @classmethod
    def initialize(cls, _db):
        cls.db = _db
        metadata_wrangler_url = os.environ['METADATA_WEB_APP_URL']
        cls.metadata_client = SimplifiedOPDSLookup(metadata_wrangler_url)

app = Flask(__name__)
app.config['DEBUG'] = True
app.debug = True

@app.route('/staff-picks', methods=['GET', 'POST'])
def staff_picks():    
    error = ""

    def _form(error):
        if error:
            error = '<h2>Error: %s</h2>' % error
        else:
            error = ''
        return '''<!doctype html>
<title>Update Staff Picks</title>
<h1>Update Staff Picks</h1>
%(error)s
<form action="" method="post" enctype="multipart/form-data">
  <p><input type="file" name="file">
     <input type="submit" value="Update">
</form>''' % dict(error=error)

    if request.method != 'POST':
        return _form(None)

    f = request.files['file']
    if not f:
        return _form('No file selected.')
    try:
        reader = csv.DictReader(f.stream)
    except Exception, e:
        return _form('Could not read CSV file: %s' % e)
        
    list_processor = CustomListFromCSV(
        data_source_name=DataSource.LIBRARIANS,
        list_name="Staff Picks",
        metadata_client = Conf.metadata_client,
        first_appearance_field='Timestamp',
        title_field='Title',
        author_field='Author',
        isbn_field='ISBN',
        default_language='eng',
        publication_date_field='Publication Year',
        tag_fields=['Genre / Collection area'],
        audience_fields=['Age', 'Age range [children]'],
        annotation_field='Annotation',
        annotation_author_name_field='Name',
        annotation_author_affiliation_field='Location',
    )
    output = StringIO()
    writer = csv.writer(output)
    try:
        list_processor.to_customlist(_db, reader, writer)
    except csv.Error, e:
        return _form(str(e))
    headers = { "Content-type": "text/csv; charset=UTF-8; header=present" }
    return Response(output.getvalue(), 200, headers)


if os.environ.get('TESTING') == "True":
    Conf.testing = True
else:
    Conf.testing = False
    _db = production_session()
    Conf.initialize(_db)

if __name__ == '__main__':
    debug = True
    url = os.environ['CIRCULATION_WEB_APP_URL']
    scheme, netloc, path, parameters, query, fragment = urlparse.urlparse(url)
    if ':' in netloc:
        host, port = netloc.split(':')
        port = int(port)
    else:
        host = netloc
        port = 80
    print host, port
    app.run(debug=debug, host=host, port=port)
