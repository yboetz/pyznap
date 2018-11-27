# Changelog
All notable changes to pyznap will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.2] - 2018-11-27
### Added
- Catch KeyboardInterrupt exceptions.

### Changed
- Code cleanup.

## [1.1.1] - 2018-11-17
### Changed
- Changed frequency of 'frequent' snapshots to 1 minute. Interval at which 'frequent' snapshots
are taken can be controlled by cronjob. This allows users to take snapshots at different intervals
(1min, 5min, 15min, ...).
- Code cleanup in process.py. No more overwriting of subprocess functions.

### Fixed
- Fixed pv width to 100 chars.

## [1.1.0] - 2018-10-15
### Added
- pyznap now uses `pv` to show progress of zfs send operations.
- Better error handling during zfs send over ssh.

### Fixed
- Changed readme to only mention python 3.5+.

## [1.0.2] - 2018-08-15
### Added
- More verbose error messages when CalledProcessError is raised.

### Fixed
- Send over ssh would fail with OSError if dataset has no space left.

## [1.0.1] - 2018-08-13
### Added
- pyznap now checks if the dest filesystem has a 'zfs receive' ongoing before trying to send.
- Added more helpful error message when source/dest do not exist.
- Added a changelog.

### Fixed
- Fixed bug where ssh connection would be opened but not closed if dataset does not exist.

## [1.0.0] - 2018-08-10
### Added
- Added tests to test pyznap running over time.

### Changed
- Code cleanup.
- Changed some docstrings.
- Extended Readme.

### Fixed
- Fixed multilne ZFS errors not being matched.

## [0.9.1] - 2018-08-08
### Fixed 
- Logging was writing to stderr instead of stdout.

## [0.9.0] - 2018-08-07
### Added
- First release on PyPI.
