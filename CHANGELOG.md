# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.1] - 2022-09-28

### Changed in 2.1.1

- In `Dockerfile`, updated FROM instruction to `senzing/senzingapi-tools:3.3.0`
- In `requirements.txt`, updated:
  - boto3==1.24.82

## [2.1.0] - 2022-08-26

### Changed in 2.1.0

- In `Dockerfile`, bump from `senzing/senzingapi-runtime:3.1.1` to `senzing/senzingapi-runtime:3.2.0`
- Updated python dependencies

### Deleted in 2.1.0

- Deleted `Dockerfile-with-data`

## [2.0.2] - 2022-07-29

### Changed in 2.0.2

- Changed from `SENZING_AZURE_CONNECTION_STRING` to `SENZING_AZURE_QUEUE_CONNECTION_STRING` for clarity

## [2.0.1] - 2022-07-20

### Changed in 2.0.1

- In `Dockerfile`, bump from `senzing/senzingapi-runtime:3.1.0` to `senzing/senzingapi-runtime:3.1.1`

## [2.0.0] - 2022-07-14

### Changed in 2.0.0

- Migrated to `senzing/senzingapi-runtime` as Docker base images

## [1.5.5] - 2022-06-30

### Changed in 1.5.5

- Add Support for `SENZING_LICENSE_BASE64_ENCODED`
- Upgrade `Dockerfile` to `FROM debian:11.3-slim@sha256:f6957458017ec31c4e325a76f39d6323c4c21b0e31572efa006baa927a160891`

## [1.5.4] - 2022-06-08

### Changed in 1.5.4

- Upgrade `Dockerfile` to `FROM debian:11.3-slim@sha256:06a93cbdd49a265795ef7b24fe374fee670148a7973190fb798e43b3cf7c5d0f`

## [1.5.3] - 2022-05-06

### Changed in 1.5.3

- Added `libodbc1` to Dockerfile

## [1.5.2] - 2022-04-28

### Changed in 1.5.2

- Fix datatype error on `SENZING_THREADS_PER_READ_PROCESS`
- Migrated URLs from "master" to "main" GitHub branches

## [1.5.1] - 2022-04-19

### Changed in 1.5.1

- Added additional logging around calls to Senzing

## [1.5.0] - 2022-04-08

### Changed in 1.5.0

- In `Dockerfile-with-data`, added docker build args to Dockerfile for more flexibility.
  - Migrate from `senzingdata-v2` to `senzingdata-v3`
  - SENZING_APT_REPOSITORY_URL
  - SENZING_DATA_PACKAGE_NAME
  - SENZING_DATA_SUBDIRECTORY

## [1.4.7] - 2022-03-18

### Changed in 1.4.7

- Support for `libcrypto` and `libssl`

## [1.4.6] - 2022-02-25

### Changed in 1.4.6

- Support for enhanced v3 python package styles

## [1.4.5] - 2022-02-11

### Changed in 1.4.5

- Improved support for Senzing v2 and v3 python package styles

## [1.4.4] - 2022-02-09

### Changed in 1.4.4

- Updated base image sha in -with-data dockerfile

## [1.4.3] - 2022-02-07

### Changed in 1.4.3

- Update to use Debian 11.2
- restructured to use multistage build.
- added docker-build-with-data target to makefile

## [1.4.2] - 2022-02-04

### Changed in 1.4.2

- Support for Senzing v2 and v3 python package styles

## [1.4.1] - 2021-10-11

### Changed in 1.4.1

- Updated to senzing/senzing-base:1.6.2

## [1.4.0] - 2021-09-21

### Added in 1.4.0

- Added subcommands for Azure Queue and Azure SQL Database:
  - `read-from-azure-queue-withinfo`
  - `read-from-azure-queue`
  - `redo-withinfo-azure-queue`
  - `write-to-azure-queue`

## [1.3.10] - 2021-08-12

### Added in 1.3.10

- Create a `Dockerfile-with-data` which embeds `/opt/senzing/data`

## [1.3.9] - 2021-07-15

### Added in 1.3.9

- Updated to senzing/senzing-base:1.6.1

## [1.3.8] - 2021-07-13

### Added in 1.3.8

- Updated to senzing/senzing-base:1.6.0

## [1.3.7] - 2021-06-09

### Added in 1.3.7

- Redacting passwords when logging the contents of engine_configuration_json
- Ensuring that a record pulled from an SQS queue that fails to load is not removed from the queue so it will be retried/dead lettered.
- Improving how the RabbitMQ virtual host default is handled

## [1.3.6] - 2021-5-12

### Added in 1.3.6

- Exposing RabbitMQ virtual host as a settable parameter.
- If the connection to the RabbitMQ exchange/server is lost, redoer now attempts to reconnect.
- Reading redo records from RabbitMQ is now more robust against record loss if a container goes down unexpectedly.
- Updating senzingdata dependency to 2.0.0

## [1.3.5] - 2021-02-18

### Added in 1.3.5

- Retry connecting to database.
- Added `endpoint_url` in AWS SQS configuration.

## [1.3.4] - 2020-12-03

### Added in 1.3.4

- Support for `SENZING_RUN_GDB`
- Additional debug statements

## [1.3.3] - 2020-09-25

### Added in 1.3.3

- Support for RabbitMQ exchanges
- Support for `SENZING_EXIT_ON_THREAD_TERMINATION`
- Support for stack traces using `gdb`.

## [1.3.2] - 2020-08-29

### Added in 1.3.2

- Support for Senzing Govnernor

## [1.3.1] - 2020-07-23

### Added in 1.3.1

- Upgrade to senzing/senzing-base:1.5.2

## [1.3.0] - 2020-06-24

### Added in 1.3.0

- Add AWS SQS support

## [1.2.1] - 2020-03-19

### Added in 1.2.1

- Add Kafka support

### Fixed in 1.2.1

- Improve RabbitMQ support
- Improve log messages
- Change from processRedoRecordWithinfo() to processWithinfo()

## [1.2.0] - 2020-03-01

### Added in 1.2.0

- Support for `withinfo`

### Changed in 1.2.0

- Use of Mixins in code architecture

## [1.1.0] - 2020-01-31

### Changed in 1.1.0

- Update to senzing/senzing-base:1.4.0

## [1.0.0] - 2020-01-21

### Added to 1.0.0

- Initial functionality:  Pull from Senzing redo queue via `G2Engine.getRedoRecord()`, push to senzing `G2Engine.process()`.
- Has 3 types of threads:  Monitor, Pull from Senzing Redo queue, Push to Senzing "process".
