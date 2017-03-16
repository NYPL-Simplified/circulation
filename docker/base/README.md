# Supported tags and respective `Dockerfile` links

- `1.1.20`, `1.1`, `latest` [(1.0.0/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/master/base/Dockerfile)
- `1.0`

Older versions of the Circulation Manager are not currently supported.

This image is updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

This particular image builds a foundation for other Circulation Manager containers by acquiring the appropriate version of the codebase, creating a virtual environment, and installing required libraries. It could be successfully used as a base for new build repositories or for light development or exploratory work.

## Using This Image
You will need:
- **A configuration file** created using JSON and the keys and values described at length [here](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration). If you're unfamiliar with JSON, we highly recommend taking the time to confirm that your configuration file is valid.

With your configuration file stored on the host, you are ready to run:
```
$ docker run --name base -it \
    -v FULL_PATH_TO_YOUR_CONFIGURATION_FILE_DIRECTORY:/etc/circulation \
    nypl/circ-base
```

You will need to detach from the generated TTY with `Ctrl`+P, `Ctrl`+Q to keep your container running.

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-base/).)
