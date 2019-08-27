# README #

pyznap is a ZFS snapshot management tool. It automatically takes and deletes snapshots and can send
them to different backup locations. You can specify a policy for a given filesystem in the
pyznap.conf file and then use cron to let it run regularly. pyznap includes zfs
bindings for python, forked and modified from https://bitbucket.org/stevedrake/weir/.


#### How does it work? ####

pyznap regularly takes and deletes snapshots according to a specified policy. You can take frequent,
hourly, daily, weekly, monthly and yearly snapshots. 'frequent' snapshots can be taken up to once
per minute, the frequency can be adjusted by the cronjob frequency. Old snapshots are deleted as
you take new ones, thinning out the history as it gets older.

Datasets can also be replicated to other pools on the same system or remotely over ssh. After an
initial sync, backups will be done incrementally as long as there are common snapshots between the
source and the destination.


#### Requirements ####

pyznap is written in python 3.5+ and has no dependencies. For developing and running the tests you
need:

    pytest
    pytest-dependency
    paramiko

You also need the `faketime` program for some tests to simulate pyznap running over time.

I suggest installing [virtualenv & virtualenvwrapper](http://docs.python-guide.org/en/latest/dev/virtualenvs/),
so you don't clutter your system python installation with additional packages.

pyznap uses `mbuffer` and `lzop` (by default) to speed up zfs send/recv, and `pv` to show progress, 
but also works if they are not installed. Other supported compression methods are: `none`, `lz4`, 
`gzip`, `pigz`, `bzip2` and `xz`.

Note that ZFS needs root access to run commands. Due to this you should install pyznap under your
root user.


#### How do I set it up? ####

pyznap can easily be installed with pip. In your virtualenv just run

    pip install pyznap

and pyznap & its requirements will be installed. This should also create an executable in your PATH,
either at `/path/to/virtualenv/pyznap/bin/pyznap` or `/usr/local/bin/pyznap`. If you use your
system python installation you might want to use the `--user` flag. In this case the executable will
be located at `~/.local/bin/pyznap`.

Before you can use pyznap, you will need to create a config file. For initial setup run

    pyznap setup [-p PATH]

This will create a directory `PATH` (default is `/etc/pyznap/`) and copy a sample config there. A
config for your system might look like this (remove the comments):

    [rpool/filesystem]
    frequent = 4                          # Keep 4 frequent snapshots
    hourly = 24                           # Keep 24 hourly snapshots
    daily = 7                             # Keep 7 daily snapshots
    weekly = 4                            # Keep 4 weekly snapshots
    monthly = 6                           # Keep 6 monthly snapshots
    yearly = 1                            # Keep 1 yearly snapshot
    snap = yes                            # Take snapshots on this filesystem
    clean = yes                           # Delete old snapshots on this filesystem
    dest = backup/filesystem              # Backup this filesystem on this location
    exclude = rpool/filesystem/data/*     # Exclude these datasets for pyznap send

Then set up a cronjob by creating a file under `/etc/cron.d/`

    nano /etc/cron.d/pyznap

and let pyznap run regularly by adding the following lines

    SHELL=/bin/sh
    PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

    */15 * * * *   root    /path/to/pyznap snap >> /var/log/pyznap.log 2>&1

This will run pyznap every quarter hour to take and delete snapshots. 'frequent' snapshots can be
taken up to once per minute, so adjust your cronjob accordingly.

If you also want to send your filesystems to another location you can add a line

    0 0 * * *   root    /path/to/pyznap send >> /var/log/pyznap.log 2>&1

This will backup your data once per day at 12pm.

You can also manage, send to and pull from remote ssh locations. Always specify ssh locations with

    ssh:port:user@host:rpool/data

A sample config which backs up a filesystem to a remote location looks like:

    [rpool/data]
    hourly = 24
    snap = yes
    clean = yes
    dest = ssh:22:user@host:backup/data   # Specify ssh destination
    dest_keys = /home/user/.ssh/id_rsa    # Provide key for ssh login. If none given, look in home dir
    compress = gzip

To pull a filesystem from a remote location use:

    [ssh::user@host:rpool/data]
    key = /home/user/.ssh/id_rsa          # Provide key for ssh login. If none given, look in home dir
    dest = tank/data
    compress = lz4

You can exclude datasets when sending using [Unix shell-style wildcards](https://docs.python.org/3/library/fnmatch.html):

    [rpool]
    dest = backup/rpool, tank/rpool
    exclude = rpool/data rpool/home/*/documents rpool/home/user1, rpool/home*

Note that exclude rules are separated by a `,` for the different dests, and you can specify multiple
rules for a single dest by separating them with a space. Exclude rules thus cannot contain any comma
or whitespace.

I would also suggest making sure that root has ownership for all files, s.t. no user can modify them.
If that is not the case just run

    chown root:root -R /etc/pyznap/


#### Command line options ####

Run `pyznap -h` to see all available options.

+ --config

  Specify config file. Default is `/etc/pyznap/pyznap.conf`.

+ -v, --versbose

  Print more verbose output.

+ setup [-p PATH]

  Initial setup. Creates a config dir and puts a sample config file there. You can specify the path
  to the config dir with the `-p` flag, default is `/etc/pyznap/`.

+ snap

  Interface to the snapshot management tool. Has three optional arguments:

  + --take

    Takes snapshots according to policy in the config file.

  + --clean

    Deletes old snapshots according to policy.

  + --full

    First takes snapshots, then deletes old ones. Default when no other option is given.

+ send

  Interface to the zfs send/receive tool. Has two usages:

  + No further option is given

    Send snapshots to backup locations according to policy.

  + -s SOURCE -d DESTINATION [-c COMPRESSION] [-i KEYFILE] [-j SOURCE_KEY] [-k DEST_KEY] [-e EXCLUDE]

    Send source filesystem to destination filesystem. If either source OR dest is a remote location,
    you can specify the keyfile with the `-i` flag. If both source AND dest are remote, you specify
    the keyfiles with the flag `-j` for the source and `-k` for the dest. You can also turn on
    compression with the `-c` flag. Currently supported options are: `none`, `lzop`, `lz4`, `gzip`,
    `pigz`, `bzip2` and `xz`. If no option is given, `lzop` is used if available. You can specify
    multiple (whitespace separated) wildcard exclude rules with the `-e` flag. Note that you should
    probably pass these as strings or escape the wildcard (e.g. `-e '*/data'` or `-e \*/data`), else
    your shell might expand the pattern.


#### Usage examples ####

+ Take snapshots according to policy in default config file:

    `pyznap snap --take`

+ Clean snapshots according to policy in another config file:

    `pyznap --config /etc/pyznap/data.conf snap --clean`

+ Take and clean snapshots according to policy in default config file:

    `pyznap snap`

+ Backup snapshots according to policy in default config file:

    `pyznap send`

+ Backup a single filesystem locally:

    `pyznap send -s tank/data -d backup/data`

+ Send a single filesystem to a remote location, using `pigz` compression:

    `pyznap send -s tank/data -d ssh:20022:root@example.com:backup/data -i /root/.ssh/id_rsa -c pigz`

+ Pull a single filesystem from a remote location:

    `pyznap send -s ssh::root@example.com:tank/data -d backup/data -c lz4`

+ Pull a single filesystem from a remote location and send it to another remote location:

    `pyznap send -s ssh::root@example1.com:tank/data -d ssh::root@example2.com:backup/data -j /root/.ssh/id_rsa_1 -k /root/.ssh/id_rsa_2`
