#!/usr/bin/python3

import subprocess
import argparse
import os
import time
import datetime
import sqlite3
import math

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

class SnapsDB:
	def __init__(self, dbpath):
		self.dbpath = dbpath
		self.newdb = not os.path.exists(dbpath)
		self.con = None

	def open(self, force_new=False):
		if force_new and not self.newdb:
			os.unlink(self.dbpath)
			self.newdb = True
		self.con = sqlite3.connect(self.dbpath)
		if not self.newdb:
			self.get_statuses()
		return self

	def close(self):
		self.con.close()

	def create_dictarray(self, cursor, keys):
		result = []
		for row in cursor:
			new_dict = {};
			for i, v in enumerate(keys):
				new_dict[v] = row[i]
			result.append(new_dict);
		return result

	def setup(self):
		# create tables and stuff
		cur = self.con.cursor()

		cur.execute('CREATE TABLE snapshot (filename VARCHAR(127) NOT NULL, time TIMESTAMP, id_snapshot_status int);')
		cur.execute('CREATE TABLE snapshot_status (status VARCHAR(20) NOT NULL);')
		cur.execute('CREATE TABLE schedule (name VARCHAR(127) NOT NULL, datefmt VARCHAR(127) NOT NULL, keep int, precedence);')
		cur.execute('CREATE TABLE relationship (id_snapshot INT, id_schedule INT, datestr VARCHAR(20) NOT NULL, id_relationship_status INT);')
		cur.execute('CREATE TABLE relationship_status (status VARCHAR(20) NOT NULL);')


		cur.executemany("INSERT INTO schedule VALUES (?,?,?,?);", [
			('regular',   '%Y%m%d%H%M%S', 10, 100),
			('hourly',    '%Y%m%d%H',     36, 200),
			('daily',     '%Y%m%d',       11, 300),
			('weekly',    '%Yw%W',         6, 400),
			('monthly',   '%Y%m',          6, 500),
			('quarterly', '%Yq%q',         6, 600),
			('yearly',    '%Y',            4, 700),
		])
		cur.executemany("INSERT INTO snapshot_status VALUES (?)", [
			("created",),
			("deleted",),
			("failed",),
			("missing",)
		])
		cur.executemany("INSERT INTO relationship_status VALUES (?)", [
			("active",),
			("expired",)
		])
		self.con.commit()
		self.get_statuses()

	def get_statuses(self):
		cur = self.con.cursor()
		cur.execute("SELECT rowid,status FROM snapshot_status")
		self.status = {}
		for row in cur:
			self.status[row[1]] = row[0]
		cur.execute("SELECT rowid,status FROM relationship_status")
		self.relationship_status = {}
		for row in cur:
			self.relationship_status[row[1]] = row[0]

	def schedules(self):
		cur = self.con.cursor()
		cur.execute("SELECT rowid,* FROM schedule ORDER BY precedence;")
		return self.create_dictarray(cur, ['id', 'name', 'datefmt', 'keep', 'precedence'])

	def helper_build_extra(self, status_opts, lookup, append_line):
		out_a = []
		out_p = []
		for s in status_opts:
			if s in lookup:
				out_a.append(append_line)
				out_p.append(lookup[s])
		if len(out_a):
			return (" AND ( {} ) ".format(" OR ".join(out_a)), out_p)
		return ("", [])

	def snapshot_create(self, path, now):
		cur = self.con.cursor()
		cur.execute("INSERT INTO snapshot (filename, time, id_snapshot_status) VALUES (?, ?, ?)", (path, int(time.mktime(now)), self.status["created"]))
		self.con.commit()
		return cur.lastrowid

	def snapshot_get(self, id_snapshot):
		cur = self.con.cursor()
		if type(id_snapshot) is not list: id_snapshot = [id_snapshot]
		cur.execute("SELECT rowid,* FROM snapshot WHERE rowid IN ({});".format(','.join(['?']*len(id_snapshot))), id_snapshot)
		return self.create_dictarray(cur, ['id' ,'filename', 'time', 'id_snapshot_status'])

	def snapshot_expire(self, id_snapshot):
		cur = self.con.cursor()
		params = [self.status['deleted']]
		if type(id_snapshot) is not list: params.append(id_snapshot)
		else: params += id_snapshot
		cur.execute("UPDATE snapshot SET id_snapshot_status = ? WHERE rowid IN ({});".format(','.join(['?']*len(id_snapshot))), params)
		self.con.commit()

	def snapshot_count_since(self, time):
		cur = self.con.cursor()
		cur.execute("SELECT COUNT(rowid) FROM `snapshot` WHERE `time` > ?", (int(time),))
		return cur.fetchone()[0]

	def relationship_exists(self, id_sch, datestr):
		cur = self.con.cursor()
		cur.execute("SELECT COUNT(rowid) FROM `relationship` WHERE `id_schedule` == ? AND `datestr` == ? LIMIT 1", (id_sch, datestr))
		return bool(cur.fetchone()[0])

	def relationship_create(self, id_snap, id_sch, datestr):
		cur = self.con.cursor()
		cur.execute("INSERT INTO relationship (id_snapshot, id_schedule, datestr, id_relationship_status) VALUES (?, ?, ?, ?)", (id_snap, id_sch, datestr, self.relationship_status["active"]))
		self.con.commit()
		return cur.lastrowid

	def relationship_find_by_schedule(self, id_sch, ignore=0, status_opts=("active","created")):
		cur = self.con.cursor()

		extra_snapshot     = self.helper_build_extra(status_opts, self.status,              "snapshot.id_snapshot_status = ?")
		extra_relationship = self.helper_build_extra(status_opts, self.relationship_status, "relationship.id_relationship_status = ?")
		extras = ""       + extra_snapshot[0] + extra_relationship[0]
		params = [id_sch] + extra_snapshot[1] + extra_relationship[1] 

		offsets = ""
		if ignore:
			offsets = "OFFSET ?"
			params.append(ignore);

		cur.execute("""
			SELECT relationship.rowid, relationship.* from relationship
			LEFT JOIN snapshot ON relationship.id_snapshot = snapshot.rowid
			WHERE relationship.id_schedule = ?
			{}
			ORDER BY snapshot.time DESC
			LIMIT -1 {}
		""".format(extras, offsets), tuple(params))
		return self.create_dictarray(cur, ['id' ,'id_snapshot', 'id_schedule', 'datestr', 'id_relationship_status'])

	def relationship_find_by_snapshot_count(self, id_snap, status_opts=("active","created")):
		cur = self.con.cursor()

		extra_snapshot     = self.helper_build_extra(status_opts, self.status,              "snapshot.id_snapshot_status = ?")
		extra_relationship = self.helper_build_extra(status_opts, self.relationship_status, "relationship.id_relationship_status = ?")
		extras = ""        + extra_snapshot[0] + extra_relationship[0]
		params = [id_snap] + extra_snapshot[1] + extra_relationship[1] 
		cur.execute("""
			SELECT count(relationship.rowid) FROM relationship
			LEFT JOIN snapshot ON relationship.id_snapshot = snapshot.rowid
			WHERE relationship.id_snapshot = ?
			{}
		""".format(extras), tuple(params))
		return bool(cur.fetchone()[0])

	def relationship_status_update(self, id_relationship, status):
		cur = self.con.cursor()
		params = []
		if type(id_relationship) is not list: id_relationship = [id_relationship]
		for id in id_relationship:
			params.append((self.relationship_status[status], id));
		cur.executemany("UPDATE relationship SET id_relationship_status = ? WHERE rowid = ?", params)
		self.con.commit()

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

	db = SnapsDB(dbpath).open(force_new=False)
	if db.newdb:
		db.setup()

	g_snapshot_count = db.snapshot_count_since(today);

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
		id_snap = db.snapshot_create(name, now)

		old_relationships = []

		# Add the relationships
		for sch in schedules_applicable:
			db.relationship_create(id_snap, sch['id'], strftime_q(sch['datefmt'], now))

			# Looking for old relationships here saves us having to needlessly reopen the database afterwards.
			old_relationships += db.relationship_find_by_schedule(sch['id'], ignore=sch['keep'])

		db.close()

		new_snapshot = os.path.join(args.snapshots, name)
		# create the snapshot on the filesystem
#		print("/bin/btrfs subvol snapshot -r {} {}".format(os.path.normpath(args.subvolume), new_snapshot))
		subprocess.call(["/bin/btrfs", "subvol", "snapshot", "-r", os.path.normpath(args.subvolume), new_snapshot])

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
#				print("/bin/btrfs subvol delete {}".format(os.path.join(args.snapshots, snap['filename'])))
				subprocess.call(["/bin/btrfs", "subvol", "delete", os.path.join(args.snapshots, snap['filename'])])


	db.close()

main()
