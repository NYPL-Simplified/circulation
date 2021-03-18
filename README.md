# Library Simplified Server Core
[![Build Status](https://travis-ci.org/NYPL-Simplified/server_core.svg?branch=master)](https://travis-ci.org/NYPL-Simplified/server_core)

This is the Server Core for [Library Simplified](http://www.librarysimplified.org/). The server core contains functionality common between various LS servers, including database models and essential class constants, OPDS parsers, and certain configuration details.

The [OA Content Server](https://github.com/NYPL-Simplified/content_server), [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata-wrangler), and [Circulation Manager](https://github.com/NYPL-Simplified/circulation) all depend on this codebase. Treat it well.

## Installation & Workflow

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified repository, please review those instructions._**

More often than not, this repository is installed and altered as part of another server using the following commands:
```sh
$ git submodule init    # from inside one of the
$ git submodule update
$ cd core               # make changes to core, as needed
```

Keep in mind that this workflow requires that any changes to the server core are committed and pushed independent to changes in the parent server.

Should you need to work on the core alone, use a traditional git workflow:
```sh
$ git clone git@github.com:NYPL/Simplified-server-core.git core
```

## Testing
To run `pytest` unit tests locally, install `tox`.

```
pip install tox
```

If you have all the services used by the tests setup, you can simply run the `tox` command and it will run the tests.

The following commands start all the necessary services in docker containers. If you already have elastic search or postgres running locally, you don't need to run the elastic search or db docker commands.

```
# Start the containers for testing
docker run -d -p 9005:5432/tcp --name db -e POSTGRES_USER=simplified_test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=simplified_circulation_test postgres:9.6
docker run -d -p 9006:9200/tcp --name es -e discovery.type=single-node elasticsearch:6.8.6
docker run -d -p 9007:9000/tcp --name minio -e MINIO_ACCESS_KEY=simplified -e MINIO_SECRET_KEY=12345678901234567890 bitnami/minio:latest

# Add elasticsearch plugin
docker exec es elasticsearch-plugin install -s analysis-icu
docker restart es
```

If you already have elastic search or postgres running locally, make sure that the localhost port is updated in the following commands:

```
# Set environment variables
export SIMPLIFIED_TEST_DATABASE="postgres://simplified_test:test@localhost:9005/simplified_circulation_test"
export SIMPLIFIED_TEST_ELASTICSEARCH="http://localhost:9006"
export SIMPLIFIED_TEST_MINIO_ENDPOINT_URL="http://localhost:9007"
export SIMPLIFIED_TEST_MINIO_USER="simplified"
export SIMPLIFIED_TEST_MINIO_PASSWORD="12345678901234567890"

# Run tox
tox
```

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
