# Library Simplified Server Core

This is the Server Core for [Library Simplified](http://www.librarysimplified.org/). The server core contains functionality common between various LS servers, including database models and essential class constants, OPDS parsers, and certain configuration details.

The [OA Content Server], [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata-wrangler), and Circulation Manager all depend on this codebase. Treat it well.

## Installation & Workflow

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified-iOS/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified repository, please review those instructions._**

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
