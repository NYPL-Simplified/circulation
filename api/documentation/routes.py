from api.app import app
from api.documentation.controller import OpenAPIController


@app.route('/documentation')
def generate_documentation():
    return OpenAPIController.generateSpec()
