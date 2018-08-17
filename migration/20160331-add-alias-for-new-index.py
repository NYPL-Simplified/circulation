#!/usr/bin/env python
"""
Add an alias to a new search index.

Process for creating and switching to the new index:
Deploy the new code.
Run this migration, which creates a new index ("-v2") and an alias ("-current")
based on the current index name.
Run `bin/repair/search_index <new_index_name>`.
Change the config file to point to the alias instead of the old index name.
Restart the application.

The old index can be dropped when we're confident the new index works.
"""
import os
import sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
from core.scripts import Script
from core.external_search import ExternalSearchIndex
from api.config import Configuration


class AddSearchIndexAlias(Script):

    def do_run(self):
        integration = Configuration.integration(
            Configuration.ELASTICSEARCH_INTEGRATION,
        )
        old_index = integration.get(
            Configuration.ELASTICSEARCH_INDEX_KEY,
        )
        new_index = old_index + "-v2"
        alias = old_index + "-current"

        search_index_client = ExternalSearchIndex(works_index=new_index)
        search_index_client.indices.put_alias(
            index=new_index,
            name=alias
        )

AddSearchIndexAlias().run()
