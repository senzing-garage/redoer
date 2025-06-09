# redoer development

The following instructions are used when modifying and building the Docker image.

## Prerequisites for development

:thinking: The following tasks need to be complete before proceeding.
These are "one-time tasks" which may already have been completed.

1. The following software programs need to be installed:
   1. [git]
   1. [make]
   1. [docker]

## Clone repository

For more information on environment variables, see [Environment Variables].

1. Set these environment variable values:

   ```console
   export GIT_ACCOUNT=senzing
   export GIT_REPOSITORY=redoer
   export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
   export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
   ```

1. Using the environment variables values just set, follow steps in [clone-repository] to install the Git repository.

## Build Docker image

1. **Option #1:** Using `docker` command and GitHub.

   ```console
   sudo docker build \
     --tag senzing/redoer \
     https://github.com/senzing-garage/redoer.git#main
   ```

1. **Option #2:** Using `docker` command and local repository.

   ```console
   cd ${GIT_REPOSITORY_DIR}
   sudo docker build --tag senzing/redoer .
   ```

1. **Option #3:** Using `make` command.

   ```console
   cd ${GIT_REPOSITORY_DIR}
   sudo make docker-build
   ```

   Note: `sudo make docker-build-development-cache` can be used to create cached Docker layers.

[clone-repository]: https://github.com/senzing-garage/knowledge-base/blob/main/HOWTO/clone-repository.md
[docker]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/docker.md
[Environment Variables]: https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md
[git]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/git.md
[make]: https://github.com/senzing-garage/knowledge-base/blob/main/WHATIS/make.md
