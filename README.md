# Library Simplified Circulation Manager
[![Build Status](https://travis-ci.org/NYPL-Simplified/circulation.svg?branch=master)](https://travis-ci.org/NYPL-Simplified/circulation)

This is the Circulation Manager for [Library Simplified](http://www.librarysimplified.org/). The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

It depends on the [LS Server Core](https://github.com/NYPL-Simplified/server_core) as a git submodule.

## Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

Keep in mind that the metadata server requires unique database names and elasticsearch, as detailed below.

### Database

Create relevant databases in Postgres:
```sh
$ sudo -u postgres psql
CREATE DATABASE simplified_circulation_test;
CREATE DATABASE simplified_circulation_dev;

# Create users, unless you've already created them for another LS project
CREATE USER simplified with password '[password]';
CREATE USER simplified_test with password '[password]';

grant all privileges on database simplified_circulation_dev to simplified;
grant all privileges on database simplified_circulation_test to simplified_test;
```

### Elasticsearch

Install Elasticsearch.

*On Linux:*
  ```sh
  $ sudo apt-get install openjdk-7-jre
  ```
  Then follow the instructions here: https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-repositories.html

*Or, on a brew-capable Mac:*
  ```sh
  $ brew tap caskroom/cask
  $ brew install brew-cask
  $ brew cask install java
  $ brew install homebrew/versions/elasticsearch17
  ```

### Front-end admin interface

To include the admin interface, set "include_admin_interface": "true" in the config file, and install the node module for the front-end application.

To use the published version, run `npm install` from api/admin.

To use a local version, clone circulation-web and link it to this project.

From circulation-web: `npm link`
From circulation's api/admin directory: `npm link simplified-circulation-web`

## License

```
Copyright © 2015 The New York Public Library, Astor, Lenox, and Tilden Foundations

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
