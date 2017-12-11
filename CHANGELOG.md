This document contains release notes for the current development
release of the circulation manager. When a release is tagged, release
notes are removed from here and moved to [the release page](https://github.com/NYPL-Simplified/circulation/releases/).

## Circulation changes

* The simple authentication provider can authenticate multiple test
  identifiers, so long as all of them have the same password.

## Core changes

* Performance improvement: Lane size is calculated ahead of time and
  stored in the database.

* Connector to the Odilo catalog/circulation API.
