# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2020-01-31

### Changed in 1.1.0

- Update to senzing/senzing-base:1.4.0

## [1.0.0] - 2020-01-21

### Added to 1.0.0

- Initial functionality:  Pull from Senzing redo queue via `G2Engine.getRedoRecord()`, push to senzing `G2Engine.process()`.
- Has 3 types of threads:  Monitor, Pull from Senzing Redo queue, Push to Senzing "process".
