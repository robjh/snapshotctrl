
import actor_base
import subprocess
import os

class Actor_Btrfs(actor_base.Actor_Base):
	def __init__(self, args):
		super().__init__(args)

	def create(self, name):
#		print("/bin/btrfs subvol snapshot -r {} {}".format(os.path.normpath(self.subvolume), os.path.join(self.snapshots, name)))
		subprocess.call(["/bin/btrfs", "subvol", "snapshot", "-r", os.path.normpath(self.path_target), os.path.join(self.path_snapshots, name)])

	def delete(self, name):
#		print("/bin/btrfs subvol delete {}".format(os.path.join(self.snapshots, snap['filename'])))
		subprocess.call(["/bin/btrfs", "subvol", "delete", os.path.join(self.path_snapshots, name)])

