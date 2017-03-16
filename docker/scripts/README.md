# Supported tags and respective `Dockerfile` links

- `1.1.20`, `1.1`, `latest` [(1.1.20/Dockerfile)](https://github.com/NYPL-Simplified/circulation-docker/blob/master/scripts/Dockerfile)
- `1.0`

Older versions of the Circulation Manager are not currently supported.

This image is updated via [pull requests to the `NYPL-Simplified/circulation-docker` GitHub repo](https://github.com/NYPL-Simplified/circulation-docker/pulls).

## What is the Circulation Manager?

The circulation manager is the main connection between a library's collection and Library Simplified's various client-side applications. It handles user authentication, combines licensed works with open access content from the [OA Content Server](https://github.com/NYPL-Simplified/content_server), pulls in updated book information from the [Metadata Wrangler](https://github.com/NYPL-Simplified/metadata_wrangler), and serves up available books in appropriately organized OPDS feeds.

This particular image builds containers to handle automated scripts on the Circulation Manager, at [the recommended times and frequencies](https://github.com/NYPL-Simplified/Simplified/wiki/AutomatedJobs#circulation-manager).

## Using This Image
You will need:
- **A configuration file** created using JSON and the keys and values described at length [here](https://github.com/NYPL-Simplified/Simplified/wiki/Configuration). If you're unfamiliar with JSON, we highly recommend taking the time to confirm that your configuration file is valid.
- **Your local timezone**, selected according to [Debian-system timezone options](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). This will allow timed scripts intended to run at hours of low usage to run in accordance with your local time.

With your time zone value and the configuration file stored on the host, you are ready to run:
```
$ docker run --name scripts \
    -d -e TZ="YOUR_TIMEZONE_STRING" \
    -e LIBSIMPLE_DB_INIT=true \                  # only when using the database for the first time
    -v FULL_PATH_TO_YOUR_CONFIGURATION_FILE_DIRECTORY:/etc/circulation \
    nypl/circ-scripts
```

For troubleshooting information and installation directions for the entire Circulation Manager tool suite, please review [the full deployment instructions](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker).

## Additional Configuration

If you are familiar with the LS Circulation Manager's automated job processes and would like to incorporate your own crontab, you are welcome to do so. If your changes won't overlap with existing scripts, it can be copied into a running container as follows:
`$ docker cp PATH_TO_YOUR_NEW_CRONTAB scripts:/etc/cron.d/`

However, if you intend to replace the existing crontab, you will need to inject the file into a new container:
```
$ docker run --name scripts \
    -d -e TZ="YOUR_TIMEZONE_STRING" \
    -v FULL_PATH_TO_YOUR_CONFIGURATION_FILE_DIRECTORY:/etc/circulation \
    -v FULL_PATH_TO_DIRECTORY_WITH_YOUR_NEW_CRONTAB:/etc/cron.d \
    nypl/circ-scripts
```

## Contributing

We welcome your contributions to new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a [GitHub issue](https://github.com/NYPL-Simplified/circulation-docker/issues/new), especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.


(**Note:** This README is intended to directly reflect [the documentation on Docker Hub](https://hub.docker.com/r/nypl/circ-scripts/).)
