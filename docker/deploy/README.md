# Supported tags and respective `Dockerfile` links

- `1.1.20`, `1.1`, `latest` [(1.1/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/master/deploy/Dockerfile)
- `1.0`

Older versions of the Circulation Manager are not currently supported.

This image is updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

This particular image builds containers to deploy the Circulation Manager API [using Nginx and uWSGI](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Nginx-&-uWSGI).

## Using This Image
You will need:
- **A configuration file** created using JSON and the keys and values described at length [here](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration). If you're unfamiliar with JSON, we highly recommend taking the time to confirm that your configuration file is valid.
- **An exposed port 80** on your host machine.
- **A hosted elasticsearch v1 instance**, included in your configuration file. You may choose to use Docker to host this instance. If so, further instructions can be found [here](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

With your the exposed port and the complete configuration file stored on the host, you are ready to run:
```
$ docker run --name deploy \
    -d -p 80:80 \
    -v FULL_PATH_TO_YOUR_CONFIGURATION_FILE_DIRECTORY:/etc/circulation \
    -e LIBSIMPLE_DB_INIT=true \                  # only when using the database for the first time
    nypl/circ-deploy
```

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

## Additional Configuration

If you would like to use different tools to handle deployment for the LS Circulation Manager, you are more than welcome to do so! We would love to support more deployment configurations; feel free to contribute any changes you may make to [the official Docker build repository](https://github.com/NYPL-Simplified/circulation-docker)!

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-deploy/).)
