# zfs-backup

This is a simple but flexible tool for:
* Synchronizing ZFS snapshots between local or remote filesystems
  * Supports both SSH and local access
* Cleaning out old snapshots
  * Has a simple, robust, and flexible scheduling mechanism

The guiding principles of this tool are:
* Unix philosophy
* If something is weird or not obvious, fail gracefully and let the user deal with it

## Installation

This tool is written in Haskell.

Install [Stack](https://haskellstack.org).

Clone this repo and run `stack install`.

## Usage

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
# In this case, we keep the last 10 snapshots and one week of daily snapshots
zfs-backup cleanup-snapshots --filesystem tank/my_files --most-recent 10 --also-keep 7@1-per-day
# Our remote backup keeps everything from the last 6 months and 10 years of quarterly snapshots
zfs-backup cleanup-snapshots --filesystem someuser@1234.zfs.rsync.net:data1/my_backup --also-keep 6-months --also-keep 40@4-per-year
```

This tool tries to be keep it pretty simple. Under the hood, it shells out to `ssh` and `zfs`.

All of the time-related operations (like splitting a month up into `n` parts) happen in UTC. None of these operations depend on the current time zone.

Sampling snapshots (e.g. `10@5-per-day`) do not depend on the current time, so they are stable and consistent across time.

The tool uses snapshots' `creation` time and `guid` to identify them, so you can feel free to use whatever naming scheme you like.

The tool is pretty fast during transfers (not that it has to do much work) - it can handle about 3GB/sec on my laptop, so that probably won't be an issue for anyone.


## Help Text

```bash
$ zfs-backup --help
ZFS Backup Tool

Usage: zfs-backup (list | copy-snapshots | cleanup-snapshots)

Available options:
  -h,--help                Show this help text

Available commands:
  list                     
  copy-snapshots           
  cleanup-snapshots        

$ zfs-backup list --help
Usage: zfs-backup list [--remote SSHSPEC] [--ignoring REGEX]...

Available options:
  -h,--help                Show this help text
  --remote SSHSPEC         Remote host to list on
  --ignoring REGEX...      Ignore snapshots with names matching any of these
                           regexes

$ zfs-backup copy-snapshots --help
Usage: zfs-backup copy-snapshots --src REMOTABLE FILESYSTEMNAME
                                 --dst REMOTABLE FILESYSTEMNAME
                                 [--send-compressed] [--send-raw] [--dry-run]
                                 [--ignoring REGEX]...

Available options:
  -h,--help                Show this help text
  --src REMOTABLE FILESYSTEMNAME
                           Can be "tank/set" or "user@host:tank/set"
  --dst REMOTABLE FILESYSTEMNAME
                           Can be "tank/set" or "user@host:tank/set"
  --send-compressed        Send using LZ4 compression
  --send-raw               Send Raw (can be used to securely backup encrypted
                           datasets)
  --dry-run                Don't actually do anything, just print what's going
                           to happen
  --ignoring REGEX...      Ignore snapshots with names matching any of these
                           regexes

$ zfs-backup cleanup-snapshots --help
Usage: zfs-backup cleanup-snapshots --filesystem REMOTABLE FILESYSTEMNAME
                                    [--most-recent INT] [--also-keep HISTORY]...
                                    [--dry-run] [--ignoring REGEX]...

Available options:
  -h,--help                Show this help text
  --filesystem REMOTABLE FILESYSTEMNAME
                           Can be "tank/set" or "user@host:tank/set"
  --most-recent INT        Keep most recent N snapshots
  --also-keep HISTORY...   To keep 1 snapshot per month for the last 12 months,
                           use "12@1-per-month". To keep up to 10 snapshots a
                           day, for the last 10 days, use "100@10-per-day", and
                           so on. To keep everything in the last 1.7 years, use
                           "1.7-years". Can use day, month, year. Multiple of
                           these flags will result in all the specified snaps
                           being kept. This all works in UTC time, by the way.
                           I'm not dealing with time zones.
  --dry-run                Don't actually do anything, just print what's going
                           to happen
  --ignoring REGEX...      Ignore snapshots with names matching any of these
                           regexes
```
