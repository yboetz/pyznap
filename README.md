# README #

pyznap is a ZFS snapshot management tool. It automatically takes and deletets snapshots and can send
them to different backup locations. You can specify a policy for a given filesystem in the
pyznap.conf file and then use cron to let it run once per hour. pyznap includes zfs bindings for
python, forked and modified from https://bitbucket.org/stevedrake/weir/.


#### Requirements ####

pyznap is written in python 3.6 and requires the following packages:

    configparser
    paramiko
    pytest
    pytest-dependency


#### How do I set it up? ####

Copy the config file to /etc/pyznap/pyznap.conf and specify the policy for your filesystems. A
sample config might look like this:

    [rpool/filesystem]
    hourly = 24                           # Keep 24 hourly snapshots
    daily = 7                             # Keep 7 daily snapshots
    weekly = 4                            # Keep 4 weekly snapshots
    monthly = 6                           # Keep 6 monthly snapshots
    yearly = 1                            # Keep 1 yearly snapshot
    snap = yes                            # Take snapshots on this filesystem
    clean = yes                           # Delete old snapshots on this filesystem
    dest = backup/filesystem              # Backup this filesystem on this location

Then set up a cronjob to run once an hour, e.g.

    0 * * * *   root    /path/to/pyznap.py snap >> /var/log/pyznap.log

This will run pyznap once per hour to take and delete snapshots. If you also want to send your
filesystems to another location you can create a cronjob with

    0 0 * * *   root    /path/to/pyznap.py send >> /var/log/pyznap.log

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


#### Command line options ####

+ --config

  Specify config file. Default is /etc/pyznap/pyznap.conf

+ snap

  Interface to the snapshot management tool. Has three options:

  + --take

    Takes snapshots according to policy in the config file

  + --clean

    Deletes old snapshots according to policy

  + --full

    First takes snapshots, then deletes old ones. Default when no other option is given

+ send

  Interface to the zfs send/receive tool. Has two usages:

  + No further option is given

    Send snapshots to backup locations according to policy

  + -s source -d destination

    Send source filesystem to destination filesystem
