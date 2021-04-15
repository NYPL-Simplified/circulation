# Library Simplified Circulation Manager
[![Build Status](https://travis-ci.org/NYPL-Simplified/circulation.svg?branch=main)](https://travis-ci.org/NYPL-Simplified/circulation)

This is the Circulation Manager for [Library Simplified](http://www.librarysimplified.org/). The Circulation Manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content, pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

It depends on [Library Simplified Server Core](https://github.com/NYPL-Simplified/server_core) as a git submodule.

## Installation

* [How to install Docker images](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker)
* [How to set up a development environment](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions)
* Two sets of Ansible playbooks are in development: [One developed by Minitex](https://github.com/Minitex/ansible-playbook-libsimple) and [a derivative developed by Amigos Library Services](https://github.com/alsrlw/ansible-playbook-libsimple)

## Generating Documentation

Code documentation using Sphinx can be found on [Github Pages](http://nypl-simplified.github.io/circulation/index.html). It currently documents this repo's `api` directory, `scripts` file, and the `core` directory submodule. The configuration for the documentation can be found in `/docs`.

Travis CI handles generating the `.rst` source files, generating the HTML static site, and deploying the build to the `gh-pages` branch.

To view the documentation _locally_, go into the `/docs` directory and run `make html`. This will generate the .rst source files and build the static site in `/docs/build/html`.

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
