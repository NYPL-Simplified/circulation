# Unreleased (2.1.1)

## Circulation changes

* The simple authentication provider can authenticate multiple test
  identifiers, so long as all of them have the same password.

## Core changes

* Performance improvement: Lane size is calculated ahead of time and
  stored in the database.

* Connector to the Odilo catalog/circulation API.

# 2.1.0 (Released 20171206)

## Circulation changes

* Lane setup can be configured through the administrative interface.
* * Initial lane setup depends on the languages in a library's collections.
* * Individual lanes can be hidden.
* * New lanes based on custom lists can be inserted into the default lane setup.
* New script: `bin/informational/list_collection_metadata_identifiers`.
  Lists all collections and the identifiers used to identify them
  to the metadata wrangler.

## Core changes

* Search improvements when searching for specific title or author
* Performance improvement: Find featured works using a single query
