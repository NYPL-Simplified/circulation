from sqlalchemy.orm.query import Query
from sqlalchemy.sql.functions import func

def fast_count(self):
    """Counts the results of a query without using super-slow subquery"""

    q = self.enable_eagerloads(False).statement.\
        with_only_columns([func.count()]).order_by(None)
    count = self.session.execute(q).scalar()
    return count

setattr(Query, 'fast_count', fast_count)
