from problem_details import *

class AdminNotAuthorized(Exception):
    status_code = 403
    def as_problem_detail_document(self, debug=False):
        return ADMIN_NOT_AUTHORIZED
