from hashlib import md5
import matplotlib.pyplot as plt
from buckets_def import Static_Bucket, Extend_Bucket, Linear_Bucket
from math import log2, ceil

FN_LENGTH = 12
LN_LENGTH = 14
EM_LENGTH = 38
REC_SIZE = FN_LENGTH + LN_LENGTH + EM_LENGTH
MAXROWID_LENGTH = 4

class Hash_Idx():
	def __init__(self, inDB, index_file, index_type, buckets, page_size, field):
		self.inDB = inDB
		self.index_file = index_file
		self.index_type = index_type
		self.buckets_init = buckets
		self.page_size = page_size
		self.field = field
		self.buckets = []

		self.idx_size = 0
		if not self.field:
			self.idx_size = FN_LENGTH + MAXROWID_LENGTH
		elif self.field == 1:
			self.idx_size = LN_LENGTH + MAXROWID_LENGTH
		elif self.field == 2:
			self.idx_size = EM_LENGTH + MAXROWID_LENGTH
		else:
			print('invalid field parameter')
			exit(1)

		self.num_idx = (page_size // self.idx_size) - 1

	def extract_key(self, record):
		key = ''
		if not self.field:
			key = record[:FN_LENGTH]
		elif self.field == 1:
			key = record[FN_LENGTH : -EM_LENGTH]
		else:
			key = record[-EM_LENGTH:]
		return key

	# assume we can fit the all in memory
	# read data entry and construct alteratice 2 indexes in a list
	def creat_index(self):
		indexes = []
		with open(self.inDB, 'rb+') as fd:
			rec = fd.read(REC_SIZE)
			row_id = 0
			while rec:
				# (key, rowid in bytes)
				indexes.append((self.extract_key(rec), row_id.to_bytes(4, byteorder='big')))
				rec = fd.read(REC_SIZE)
				row_id += 1
		return indexes

	def static_hash(self):
		indexes = self.creat_index()
		self.buckets = [Static_Bucket(i, self.num_idx) for i in range(self.buckets_init)]
		for key, row_id in indexes:
			i = int(md5(key.strip(b'\x00')).hexdigest(), 16) % self.buckets_init
			self.buckets[i].add((key, row_id))

	def extend_hash(self):
		indexes = self.creat_index()
		global_depth = int(log2(self.buckets_init))
		self.buckets = [Extend_Bucket(i, global_depth, self.num_idx) for i in range(self.buckets_init)]
		for key, row_id in indexes:
			i = int(md5(key.strip(b'\x00')).hexdigest(), 16) % (2 ** global_depth)
			while self.buckets[i].add((key, row_id)): # bucket i is full and need split
				new_bid = self.buckets[i].bid + 2 ** self.buckets[i].local_depth
				rest = self.buckets[i].extend()
				new_loc_dep = self.buckets[i].local_depth
				new_bucket = Extend_Bucket(new_bid, new_loc_dep, self.num_idx)
				for entry in rest:
					new_bucket.add(entry)
				if new_loc_dep == global_depth:
					self.buckets[new_bid] = new_bucket
				elif new_loc_dep < global_depth:
					ddiff = global_depth - new_loc_dep
					for step in range(2 ** ddiff):
						self.buckets[new_bid + step * (2 ** new_loc_dep)] = new_bucket
				else: # split
					global_depth += 1
					new_indexes = self.buckets[:]
					for j, buck in enumerate(self.buckets):
						if i == j:
							new_indexes.append(new_bucket)
						else:
							new_indexes.append(buck)
					self.buckets = new_indexes
					# print(int.from_bytes(row_id, 'big'))
					# print('global_depth: {}'.format(global_depth))
				i = int(md5(key.strip(b'\x00')).hexdigest(), 16) % (2 ** global_depth)
				

	def linear_hash(self):
		indexes = self.creat_index()
		level = 0
		next_split = 0
		self.buckets = [Linear_Bucket(i, self.buckets_init, self.num_idx) for i in range(self.buckets_init)]
		for key, row_id in indexes:
			curr_count = 2 ** level * self.buckets_init
			hashed = int(md5(key.strip(b'\x00')).hexdigest(), 16)
			i = hashed % curr_count
			j = hashed % (curr_count * 2)
			if i < next_split and i != j:
				i = j
			if self.buckets[i].add((key, row_id)): # new overflow page is created, split a bucket
				rest = self.buckets[next_split].split(level+1)
				# self.buckets.append(Linear_Bucket(i + curr_count, self.buckets_init, self.num_idx))
				new_bid = self.buckets[-1].bid + 1
				self.buckets.append(Linear_Bucket(new_bid, self.buckets_init, self.num_idx))
				for entry in rest:
					self.buckets[-1].add(entry)
				next_split += 1
				if next_split == curr_count:
					next_split = 0
					level += 1
					# print('level: {}, row_id: {}'.format(level, int.from_bytes(row_id, 'big')))

	# write index to file, return number of pages and buckets
	def output(self):
		page_count = []
		alias = {}
		num_unique = 0
		unique_pg_count = []
		for k, bucket in enumerate(self.buckets):
			if bucket.bid == k:
				page_count.append(len(bucket.entries))
				unique_pg_count.append(len(bucket.entries))
				num_unique += 1
			else: # should only happend for extendible hashing
				page_count.append(0)
				alias[k] = bucket.bid
		total_page = 0
		page_num = []
		for n, count in enumerate(page_count):
			if count:
				page_num.append(total_page)
				total_page += count
			else: # alias bucket
				page_num.append(page_num[alias[n]])
		# print('start writing')
		with open(self.index_file, 'wb+') as fd:
			# write directory header page(s)
			fd.write(self.field.to_bytes(1, byteorder='big'))
			fd.write(self.page_size.to_bytes(2, byteorder='big'))
			fd.write(self.idx_size.to_bytes(2, byteorder='big'))
			hold = (self.page_size - 14) // 4 # 4 bytes for page number
			num_overflow_page = 0
			if hold >= len(page_num):
				fd.write((0).to_bytes(3, byteorder='big'))
			else:
				hold_of = (self.page_size - 6) // 4
				num_overflow_page = ceil((len(page_num) - hold) / hold_of)
				fd.write((1).to_bytes(1, byteorder='big'))
				fd.write((1).to_bytes(2, byteorder='big'))
			fd.write(len(page_num).to_bytes(4, byteorder='big'))
			fd.write(hold.to_bytes(2, byteorder='big'))
			page_prefix = num_overflow_page + 1
			current_size = 14
			for p in page_num[:hold]:
				fd.write((p + page_prefix).to_bytes(4, byteorder='big'))
				current_size += 4
			fd.write((0).to_bytes(self.page_size - current_size, byteorder='big'))
			
			# write directory overflow page
			for i in range(num_overflow_page):
				if i == num_overflow_page - 1:
					fd.write((0).to_bytes(4, byteorder='big'))
				else:
					fd.write((1).to_bytes(1, byteorder='big'))
					fd.write((i+2).to_bytes(3, byteorder='big'))
				fd.write(hold_of.to_bytes(2, byteorder='big'))
				current_size = 6
				start = hold + hold_of * i
				end = start + hold_of
				if end > len(page_num):
					end = len(page_num)
				for p in page_num[start : end]:
					fd.write((p + page_prefix).to_bytes(4, byteorder='big'))
					current_size += 4
				fd.write((0).to_bytes(self.page_size - current_size, byteorder='big'))

			# write index file
			for i, bucket in enumerate(self.buckets):
				if not page_count[i]: # alias
					continue
				total_index = sum([len(lst) for lst in bucket.entries])
				for j, lst in enumerate(bucket.entries):
					if page_count[i] > j + 1:
						fd.write((1).to_bytes(1, byteorder='big'))
						fd.write((page_num[i]+page_prefix+j+1).to_bytes(3, byteorder='big'))
					else:
						fd.write((0).to_bytes(4, byteorder='big'))
					fd.write(total_index.to_bytes(2, byteorder='big'))
					fd.write(len(lst).to_bytes(2, byteorder='big'))
					current_size = 8
					for key, rowid in lst:
						fd.write(key)
						fd.write(rowid)
						current_size += self.idx_size
					fd.write((0).to_bytes(self.page_size - current_size, byteorder='big'))
		return (len(self.buckets), num_unique + page_prefix, total_page - num_unique, unique_pg_count)

	def main(self):
		if not self.index_type:
			self.static_hash()
		elif self.index_type == 1:
			self.extend_hash()
		elif self.index_type == 2:
			self.linear_hash()
		else:
			print('invalid index type')
			exit(1)
		return self.output()


if __name__ == '__main__':
	inDB = './names.db'
	pSize = 1024
	bucket = 512
	field = 0
	idx_on = {0: 'static', 1: 'extend', 2: 'linear'}
	for idx in range(3):
		print('{} hashing'.format(idx_on[idx]))
		out_file = './index_{}.db'.format(idx_on[idx])
		num_bucket, num_index_pg, num_overflow_pg, page_count = \
			Hash_Idx(inDB, out_file, idx, bucket, pSize, field).main()
		print('final number of bucket: {}, regular index pages: {}, over flow pages: {}'.\
			format(num_bucket, num_index_pg, num_overflow_pg))

		plt.figure()
		plt.title('histogram of {} hashing'.format(idx_on[idx]))
		plt.hist(page_count, bins=10)
		plt.xlabel("pages per bucket")
		plt.ylabel("buckets")
		plt.savefig('index_hist_{}_uniq.png'.format(idx_on[idx]))
