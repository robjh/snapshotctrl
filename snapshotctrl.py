#!/usr/bin/python3

import argparse
import os
import time
import datetime
import math

import snapsdb
import actor_btrfs

g_snapshot_count = 0

def parseargs():

	nameformat = "%Y-%m-%d.%{count}"

	parser = argparse.ArgumentParser(description='A tool for creating btrfs snapshots.')
	parser.add_argument('subvolume', metavar='subvol', help='The subvolume to be managed.')
	parser.add_argument('snapshots', metavar='snaps',  help='The location to store the snapshots.')
	parser.add_argument('--database', default='', help='Path to the database. (Default: ${subvolume}/.snapdata.db)')
	parser.add_argument('--name-format', default=nameformat, help='A format string to be used in creating the paths for the snapshots. This value is passed to strftime, which has been modified to add %%q (quarter) and %%{count} (the number of snapshots previously created today). (default: %(default)s)')
	return parser.parse_args()

def strftime_q(format, timestamp):
	global g_snapshot_count
	quarter = math.ceil(int(time.strftime("%m", timestamp)) / 3)
	return time.strftime(format, timestamp).replace("%q", str(quarter)).replace("%{count}", '%03d' % g_snapshot_count)

def main():
	global g_snapshot_count
	args = parseargs()

	now = time.gmtime()
	today = datetime.datetime(now.tm_year, now.tm_mon, now.tm_mday, tzinfo=None).timestamp()

	dbpath = args.subvolume
	if args.database:
		dbpath = args.database
	if os.path.isdir(dbpath):
		dbpath = os.path.join(dbpath, '.snapdata.db')

	db = snapsdb.SnapsDB(dbpath).open(force_new=False)
	if db.newdb:
		db.setup()

	g_snapshot_count = db.snapshot_count_since(today);


	actor = actor_btrfs.Actor_Btrfs(args)


	schedules_all = db.schedules()
	schedules_applicable = []
	for sch in schedules_all:
		# See if it would be appropreate to create this snapshot based on each schedule.
		datestr = strftime_q(sch['datefmt'], now)
		if not db.relationship_exists(sch['id'], datestr):
			# create the snapshot and tag it with this schedule
			schedules_applicable.append(sch)
		else:
			break

	if len(schedules_applicable):
		# create the snapshots entry
		name = strftime_q(args.name_format, now)
		id_snap = db.snapshot_create(name, time.mktime(now))

		old_relationships = []

		# Add the relationships
		for sch in schedules_applicable:
			db.relationship_create(id_snap, sch['id'], strftime_q(sch['datefmt'], now))

			# Looking for old relationships here saves us having to needlessly reopen the database afterwards.
			old_relationships += db.relationship_find_by_schedule(sch['id'], ignore=sch['keep'])

		db.close()

		# create the snapshot on the filesystem
		actor.create(name)

		to_delete = []
		if len(old_relationships):
			db.open()
			db.relationship_status_update([ rel['id'] for rel in old_relationships ], 'expired')
			seen = []
			for rel in filter(lambda rel: rel['id_snapshot'] not in seen,  old_relationships):
				seen.append(rel['id_snapshot'])
				if not db.relationship_find_by_snapshot_count(rel['id_snapshot']):
					to_delete.append(rel['id_snapshot'])

		if len(to_delete):
			snaps = db.snapshot_get(to_delete)
			db.snapshot_expire(to_delete)
			for snap in snaps:
				actor.delete(snap['filename'])


	db.close()

main()
