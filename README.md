# zfs-backup

This is a simple tool for:
* Synchronizing ZFS snapshots to local or remote backup targets
* Cleaning out old snapshots

The idea is that you have something like this in your crontab:

```cron
# Take a snapshot of your filesystem once every 15 minutes
*/15 * * * * snapshot.sh

# Synchronize snapshots to the backup disk or server every hour
0 * * * * backup.sh

# Clear out old snapshots once every day
0 0 * * * cleanup.sh
```

`snapshot.sh` looks something like this:

```bash
# You can name snapshots whatever you like. This tool does not care.
# In this case we'll label it with the current date+time
zfs snapshot tank/my_files@$(date +"%Y-%m-%d-%H-%M-%S") 
```

`backup.sh` looks like this:

```bash
# We can use --send-raw to send encrypted filesystems in their encrypted state,
# so we don't have to trust the backup server.
# "data1/my_backup" is the name of the target ZFS filesystem we're backing up to.
zfs-backup copy-snapshots --src tank/my_files --dst someuser@1234.zfs.rsync.net:data1/my_backup --send-raw
```

`cleanup.sh` looks like this:

```bash
# You specify how many backups you want to keep on a given schedule.
# In this case, we keep the last 10 snapshots, one week of daily snapshots,
# and 5 months worth of snapshots at 4 per month. 
zfs-backup cleanup-snapshots --filesystem tank/my_files --most-recent 10 --also-keep 7@1-per-day --also-keep 20@4-per-month
# Our remote backup keeps 1000 hourly snapshots and 10 years of quarterly snapshots
zfs-backup cleanup-snapshots --filesystem someuser@1234.zfs.rsync.net:data1/my_backup --also-keep 1000@24-per-day --also-keep 40@4-per-year
```

This tool tries to be keep it pretty simple. Under the hood, it shells out to `ssh` and `zfs` to figure out what it needs to know and do all the heavy lifting.

All of the time-related operations (like splitting a month up into `n` parts) happen in UTC and don't depend on any external factors (current time, local time zone, etc.) so that they're predictable and repeatable.

The tool uses snapshots' `creation` and `guid` metadata to identify them, so you can feel free to use whatever naming scheme you like.

The tool is pretty fast during transfers (not that it has to do much work) - it can handle about 3GB/sec on my laptop, which is faster than any ZFS filesystem I've seen, even on NVMe SSDs, so that probably won't be an issue for anyone.

## Installation

Clone this repo and run `stack install`.