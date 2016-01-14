# Library Simplified Circulation Manager

This is the Circulation Manager for [Library Simplified](http://www.librarysimplified.org/). The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content-server), pulls in udpated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata-wrangler), and serves up available books in appropriately organized OPDS feeds.

It depends on the [LS Server Core](https://github.com/NYPL/Simplified-server-core) as a git submodule.

## Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified-iOS/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

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
  $ brew install elasticsearch
  ```
