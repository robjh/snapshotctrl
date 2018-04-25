
class Actor_Base:
	def __init__(self, args):
		if self.__class__.__name__ == "Actor_Base":
			raise NotImplementedError
		self.path_snapshots = args.snapshots
		self.path_target = args.subvolume

	def create(self):
		raise NotImplementedError

	def delete(self):
		raise NotImplementedError


