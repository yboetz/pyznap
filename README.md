# README #

pyznap is a ZFS snapshot management tool. It automatically takes and deletets snapshots and can send
them to different backup locations. You can specify a policy for a given filesystem in the
pyznap.conf file and then use cron to let it run once per hour. pyznap includes zfs bindings for
python, forked and modified from https://bitbucket.org/stevedrake/weir/.


#### Requirements ####

pyznap is written in python 3.6 and requires the following packages:

    configparser
    paramiko

For developing and running the tests you also need:

    pytest
    pytest-dependency

I suggest installing [virtualenv & virtualenvwrapper](http://docs.python-guide.org/en/latest/dev/virtualenvs/),
so you don't clutter your system python installation with additional packages.

pyznap uses `mbuffer` to speed up zfs send/recv, but also works if it is not installed.


#### How do I set it up? ####

Navigate to the folder where you want to install pyznap, e.g. `/opt` and clone the git repository

    cd /opt
    git clone git@github.com:cythoning/pyznap.git

This will create a folder `opt/pyznap` and download all files from github. Then install the required
python packages (best in your virtualenv)

    cd pyznap
    pip install -r requirements.txt

Copy the config file to `/etc/pyznap/pyznap.conf`

    mkdir /etc/pyznap
    rsync -av /opt/pyznap/pyznap.conf /etc/pyznap/pyznap.conf

and specify the policy for your filesystems. A sample config might look like this (remove the comments):

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

    0 * * * *   root    /path/to/python /opt/pyznap/src/pyznap.py snap >> /var/log/pyznap.log

This will run pyznap once per hour to take and delete snapshots. If you also want to send your
filesystems to another location you can create a cronjob with

    0 0 * * *   root    /path/to/python /opt/pyznap/src/pyznap.py send >> /var/log/pyznap.log

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

I would also suggest giving file ownership to root for all files, s.t. no user can modify them:

    chown root:root -R /etc/pyznap
    chown root:root -R /opt/pyznap


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
