# circulation-docker
 
This is the Docker image for Library Simplified's [Circulation Manager](https://github.com/NYPL-Simplified/circulation_manager).

## Building

If you plan to work with stable versions of the Circulation Manager, we strongly recommend using the latest stable versions of circ-deploy and circ-scripts [published to Docker Hub](https://hub.docker.com/r/nypl/). However, there may come a time in development when you want to build Docker containers for a particular version of the Circulation Manager. If so, please use the instructions below.

### > `base/`
The base Dockerfile creates the underlying codebase upon which circ-deploy or circ-scripts container can be created. Creating a new version of this base is **required** before you can either deploy the app or run scripts with a particular commit / branch / version of the Circulation Manager.

From inside the directory where you've cloned this repository, run the command:
```sh
$ docker build --build-arg version=YOUR_DESIRED_BRANCH_OR_COMMIT -t circ-base:development --no-cache base/
```
You must run this command with the `--no-cache` option or the code in the container will not be updated from the last build, defeating the purpose of the build and enhancing overall confusion.

Feel free to change the image tag as you'd like, but you'll need to remember it for the next steps.

### > `deploy/` and `scripts/`
From inside your local repository, update the first line of `deploy/Dockerfile` and `scripts/Dockerfile` to `FROM: circ-base:development` (or your chosen image tag). Then run:
```sh
$ docker build -t circ-deploy:development deploy/
$ docker build -t circ-scripts:development scripts/
```

That's it! Run your containers as detailed in [the Quickstart documentation](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment:-Quickstart-with-Docker). Keep in mind that you may need to run migrations or configuration if you are using an existing version of the database.

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
