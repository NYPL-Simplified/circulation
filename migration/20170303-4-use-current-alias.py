#!/usr/bin/env python3
"""Create a -current alias for the index being used"""

import os
import sys

bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from api.config import Configuration as C                   # noqa: E402
from core.external_search import ExternalSearchIndex        # noqa: E402

C.load()
config_index = C.integration(C.ELASTICSEARCH_INTEGRATION).get(C.ELASTICSEARCH_INDEX_KEY)
if not config_index:
    print("No action taken. Elasticsearch not configured.")
    sys.exit()

search = ExternalSearchIndex()
update_required_text = (
    "\n\tConfiguration update required for given alias \"%s\".\n"
    "\t============================================\n"
    "\tReplace Elasticsearch configuration \"works_index\" value with alias.\n"
    "\te.g. \"works_index\" : \"%s\" ===> \"works_index\" : \"%s\"\n\n"
)

misplaced_alias_text = (
    "\n\tExpected Elasticsearch alias \"%s\" is being used with\n"
    "\tindex \"%s\" instead of configured index \"%s\".\n"
    "\t============================================\n"
)


alias_not_used = search.works_alias == search.works_index
if config_index == search.works_index:
    # The configuration doesn't have the alias in its configuration.
    current_alias = search.base_index_name(config_index)+search.CURRENT_ALIAS_SUFFIX

    if alias_not_used:
        # The current_alias wasn't set during initialization, indicating
        # that it's connected to a different alias. (If it didn't exist
        # already, it would have been created on this search client.)
        indices = ','.join(search.indices.get_alias(name=current_alias).keys())
        manual_steps_text = (
            "\tMANUAL STEPS:\n"
            "\t  1. Replace Elasticsearch configuration \"works_index\" value with alias.\n"
            "\t     e.g. \"works_index\" : \"%s\" ===> \"works_index\" : \"%s\"\n\n"
            "\t  2. Confirm alias \"%s\" is pointing to the preferred index.\n\n"
        )
        print(
            (misplaced_alias_text + manual_steps_text) %
            (current_alias, indices, config_index, config_index, current_alias, current_alias))
    else:
        # Initialization found or created an alias, but the configuration
        # file itself needs to be updated.
        print(
            update_required_text %
            (search.works_alias, config_index, search.works_alias))

elif 'error' not in search.indices.get_alias(name=config_index, ignore=[404]):
    # The configuration has an alias instead of an index.
    if config_index == search.works_alias:
        print("No action needed. Elasticsearch alias '%s' is properly named and configured." % config_index)
        print("Works are being uploaded to Elasticsearch index '%s'" % search.works_index)
    else:
        # The alias doesn't use the naming convention we expect. Try to create one
        # that does.
        index = search.indices.get_alias(name=config_index).keys()[0]
        current_alias = search.base_index_name(index)+search.CURRENT_ALIAS_SUFFIX
        current_alias_index = ','.join(search.indices.get_alias(name=current_alias).keys())

        if (current_alias_index != search.works_index or alias_not_used):
            # An alias with the proper naming convention exists elsewhere.
            # It will have to be manually removed or replaced.
            manual_steps_text = (
                "\tEITHER:\n\t  Remove -current alias \"%s\" from index \"%s\". "
                "\n\t  Place it on \"%s\" instead.\n"
                "\tOR:\n\t  Use -current alias \"%s\" in the configuration file"
                "\n\t  if \"%s\" is the preferred index.\n\n"
            )
            print(
                (misplaced_alias_text + manual_steps_text) %
                (current_alias, current_alias_index, index, current_alias,
                 current_alias_index, index, current_alias, current_alias_index))
        else:
            # ExternalSearchIndex.setup_current_alias() already does this,
            # so it shouldn't need to happen here.
            response = search.indices.put_alias(
                index=search.works_index, name=current_alias
            )
            print(update_required_text % (current_alias, config_index, current_alias))
else:
    # A catchall just in case. This shouldn't happen.
    print("\n\tSomething unexpected happened. Weird!")
    print("\t  - Given index (in configuration file): \t\"%s\"" % config_index)
    print("\t  - Elasticsearch index (in use): \t\t\"%s\"" % search.works_index)
    print("\t  - Elasticsearch alias (in use): \t\t\"%s\"" % search.works_alias)
    print("\n\tThe configured index should be manually set to the Elasticsearch alias\n")
    print("\tand this migration should be run again.")
