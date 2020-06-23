# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2020-06-23

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
