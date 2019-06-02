# bucket classes for manage indexes in memory
from hashlib import md5

class Static_Bucket():
	def __init__(self, i, num_idx):
		self.bid = i
		# idx_size + 2 ==> index size + index pointer size in page header
		self.num_idx = num_idx
		self.entries = [[]] # each inner list is a page
		self.curr = (0, 0) # pointer for add entry

	# add an index to this bucket, creat extra list for each overflow page
	def add(self, value):
		curr_page, counter = self.curr
		try:
			self.entries[curr_page].append(value)
		except IndexError:
			self.entries.append([value])
		counter += 1
		if counter == self.num_idx:
			curr_page += 1
			counter = 0
		self.curr = (curr_page, counter)


class Extend_Bucket():
	def __init__(self, i, local_depth, num_idx):
		self.bid = i
		self.local_depth = local_depth
		self.num_idx = num_idx
		self.entries = [[]]
		self.curr = (0, 0)
		self.key = None

	def one_key(self):
		curr = self.entries[0][0][0]
		for k, _ in self.entries[0]:
			if k != curr:
				return False
		return True

	# return Ture if current page is full and a extend is needed
	# add overflow page if only one key
	def add(self, value):
		curr_page, counter = self.curr
		if not curr_page:
			self.entries[0].append(value)
			counter += 1
			if counter == self.num_idx:
				curr_page = 1
				counter = 0
				if self.one_key():
					self.key = value[0]
			self.curr = (curr_page, counter)
			return False
		elif self.key == value[0]:
			try:
				self.entries[curr_page].append(value)
			except IndexError as e:
				self.entries.append([value])
			counter += 1
			if counter == self.num_idx:
				curr_page += 1
				counter = 0
			self.curr = (curr_page, counter)
			return False
		return True # buck is full, need extend

	# split current bucket into two and return a list of indexes need to be moved
	def extend(self):
		self.local_depth += 1
		move = []
		keep = []
		if self.key: # no need to split, either all move or no entry move
			new_bid = int(md5(self.key.strip(b'\x00')).hexdigest(), 16) % (2 ** self.local_depth)
			if new_bid == self.bid:
				return []
			else:
				for lst in self.entries:
					move += lst
				self.key = None
		else: # split, no overflow page
			for value in self.entries[0]:
				new_bid = int(md5(value[0].strip(b'\x00')).hexdigest(), 16) % (2 ** self.local_depth)
				if new_bid == self.bid:
					keep.append(value)
				else:
					move.append(value)
		self.entries = [keep]
		if len(keep) == self.num_idx:
			self.curr = (1, 0)
		else:
			self.curr = (0, len(keep))
		return move


class Linear_Bucket():
	def __init__(self, i, buckets, num_idx):
		self.bid = i
		self.buckets = buckets
		self.num_idx = num_idx
		self.entries = [[]]
		self.curr = (0, 0)

	def add(self, value):
		curr_page, counter = self.curr
		try:
			self.entries[curr_page].append(value)
		except IndexError:
			self.entries.append([value])
			counter += 1
			self.curr = (curr_page, counter)
			return True
		counter += 1
		if counter == self.num_idx:
			curr_page += 1
			counter = 0
		self.curr = (curr_page, counter)
		return False

	# split current bucket into two and return a list of indexes need to be moved
	def split(self, level):
		move = []
		keep = []
		for lst in self.entries:
			for value in lst:
				new_bid = int(md5(value[0].strip(b'\x00')).hexdigest(), 16) % (2 ** level * self.buckets)
				if new_bid == self.bid:
					keep.append(value)
				else:
					move.append(value)
		self.entries = [[]]
		self.curr = (0, 0)
		for entry in keep:
			self.add(entry)
		return move

