# Changelog
All notable changes to pyznap will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [1.4.3] - 2019-09-07
### Fixed
- pyznap would falsely assume executables such as 'pv' exist on SmartOS even when not present.


## [1.4.2] - 2019-08-30
### Fixed
- Catch DatasetNotFoundError if dataset was destroyed after starting pyznap.


## [1.4.1] - 2019-08-27
### Fixed
- Close stderr to detect broken pipe.
- Raise CalledProcessError if there is any error during zfs receive.


## [1.4.0] - 2019-08-27
### Added
- You can now exclude datasets when sending using [Unix shell-type wildcards](https://docs.python.org/3/library/fnmatch.html).
Use the `exclude` keyword in the config or the `-e` flag for `pyznap send`.


## [1.3.0] - 2019-08-22
### Added
- pyznap can now pull data over ssh, i.e. you can now send form local to local, local to remote,
remote to local and remote to remote. Note that remote to remote is not direct, but via the local
machine.
- `pv` now outputs status once per minute when stdout is redirected (e.g. to a file).

### Changed
- Rewrote local/remote 'zfs send' commands in a more uniform manner.

### Fixed
- Enforce python>=3.5 in setup.py.


## [1.2.1] - 2019-07-15
### Fixed
- Removed `configparser` dependency.


## [1.2.0] - 2019-07-14
### Added
- pyznap now uses compression for sending over ssh. Current supported methods are `none`, `lzop`
(default), `lz4`, `gzip`, `pigz`, `bzip2` and `xz`. There is a new config option (e.g. `compress = none`)
and a new flag `-c` for `pyznap send`.
- `mbuffer` is now also used on the dest when sending over ssh.

### Changed
- Rewrote how commands are executed over ssh: Implemented own SSH class, removed paramiko dependency.
- General code cleanup.


## [1.1.3] - 2019-07-14
### Fixed
- Send would fail on FreeBSD due to missing stream_size.


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
- Fixed multiline ZFS errors not being matched.


## [0.9.1] - 2018-08-08
### Fixed 
- Logging was writing to stderr instead of stdout.


## [0.9.0] - 2018-08-07
### Added
- First release on PyPI.
