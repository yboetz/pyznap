# README #

pyznap is a ZFS snapshot management tool. It automatically takes and deletes snapshots and can send
them to different backup locations. You can specify a policy for a given filesystem in the
pyznap.conf file and then use cron to let it run once per quarter-hour. pyznap includes zfs
bindings for python, forked and modified from https://bitbucket.org/stevedrake/weir/.


#### Requirements ####

pyznap is written in python 3.5+ and requires the following packages:

    configparser
    paramiko

For developing and running the tests you additionally need:

    pytest
    pytest-dependency

You also need the `faketime` program for some tests to simulate pyznap running over time.

I suggest installing [virtualenv & virtualenvwrapper](http://docs.python-guide.org/en/latest/dev/virtualenvs/),
so you don't clutter your system python installation with additional packages.

pyznap uses `mbuffer` to speed up zfs send/recv and `pv` to show progress, but also works if they
are not installed.

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
    frequent = 4                          # Keep 4 quarter-hourly snapshots
    hourly = 24                           # Keep 24 hourly snapshots
    daily = 7                             # Keep 7 daily snapshots
    weekly = 4                            # Keep 4 weekly snapshots
    monthly = 6                           # Keep 6 monthly snapshots
    yearly = 1                            # Keep 1 yearly snapshot
    snap = yes                            # Take snapshots on this filesystem
    clean = yes                           # Delete old snapshots on this filesystem
    dest = backup/filesystem              # Backup this filesystem on this location

Then set up a cronjob by opening your `crontab` file

    nano /etc/crontab

and let pyznap run regularly by adding the following line

    */15 * * * *   root    /path/to/pyznap snap >> /var/log/pyznap.log

This will run pyznap every quarter hour to take and delete snapshots. If you also want to send your
filesystems to another location you can create a cronjob with

    0 0 * * *   root    /path/to/pyznap send >> /var/log/pyznap.log

This will backup your data once per day at 12pm.

You can also manage and send to remote ssh locations. Always specify ssh locations with

    ssh:port:user@host:rpool/data

A sample config which backs up a filesystem to a remote location looks like

    [rpool/data]
    hourly = 24
    snap = yes
    clean = yes
    dest = ssh:22:user@host:backup/data   # Specify ssh destination
    dest_keys = /home/user/.ssh/id_rsa    # Provide key for ssh login. If none given, look in home dir

I would also suggest making sure that root has ownership for all files, s.t. no user can modify them.
If that is not the case just run

    chown root:root -R /etc/pyznap/


#### Command line options ####

+ --config

  Specify config file. Default is `/etc/pyznap/pyznap.conf`.

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

  + -s SOURCE -d DESTINATION [-i KEYFILE]

    Send source filesystem to destination filesystem. If destination is a ssh location you can
    specify a keyfile with the `-i` flag.
