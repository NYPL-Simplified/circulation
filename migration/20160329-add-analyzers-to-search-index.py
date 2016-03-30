#!/usr/bin/env python
"""Build a new search index using the new analyzers, and add an alias to it.
After this script runs, the config file will need to be changed to use the new
alias, and the old index can be dropped when we're confident the new index works.
"""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from api.monitor import (
    SearchIndexUpdateMonitor,
)
from core.scripts import RunMonitorScript
from core.external_search import ExternalSearchIndex
from api.config import Configuration

integration = Configuration.integration(
    Configuration.ELASTICSEARCH_INTEGRATION,
)
old_index = integration.get(
    Configuration.ELASTICSEARCH_INDEX_KEY,
)
new_index = old_index + "-v2"
alias = old_index + "-current"

search_index_client = ExternalSearchIndex(works_index=new_index)
RunMonitorScript(SearchIndexUpdateMonitor).run()
search_index_client.indices.put_alias(
    index=new_index,
    name=alias
)
