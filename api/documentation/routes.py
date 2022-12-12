from api.app import app
from api.documentation.admin_controller import AdminAPIController
from api.documentation.public_controller import PublicAPIController


@app.route('/admin_docs')
def generate_admin_documentation():
    return AdminAPIController.generateSpec()


@app.route('/public_docs')
def generate_public_documentation():
    return PublicAPIController.generateSpec()
