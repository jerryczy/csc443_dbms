from hashlib import md5
from math import log2

FN_LENGTH = 12
LN_LENGTH = 14
EM_LENGTH = 38
REC_SIZE = FN_LENGTH + LN_LENGTH + EM_LENGTH
REC_PER_PAGE = 16 # 1024 // REC_SIZE

def read_to_int(fd, size):
	return int.from_bytes(fd.read(size), byteorder='big')

def build_record(record):
	first_name = record.decode()[:FN_LENGTH].strip('\x00')
	last_name = record.decode()[FN_LENGTH : -EM_LENGTH].strip('\x00')
	email = record.decode()[-EM_LENGTH:].strip('\x00')
	return (first_name, last_name, email)

def scan(dbFile, field, value):
	with open(dbFile, 'rb+') as fd:
		counter = 0
		data = fd.read(REC_SIZE)
		while data:
			record = build_record(fd.read(REC_SIZE))
			if record[field] == value:	
				fn, ln, em = record
				print('First Name: {}, Last Name: {}, E-mail: {}'.format(fn, ln, em))
			data = fd.read(REC_SIZE)
			counter += 1
	return counter // REC_PER_PAGE

def query(dbFile, indexFile, field, value):
	page_read = 1
	data_pg_read = 0
	scan = False
	rowids = []
	# check field
	with open(indexFile, 'rb+') as fd:
		index_field = read_to_int(fd, 1)
		if index_field != field:
			data_pg_read = scan(dbFile, field, value)
			return page_read, data_pg_read

	with open(indexFile, 'rb+') as fd:
		# search for the page number of hash value
		index_field = read_to_int(fd, 1)
		page_size = read_to_int(fd, 2)
		index_size = read_to_int(fd, 2)
		overflow = read_to_int(fd, 1)
		of_pointer = read_to_int(fd, 2)
		total_buckets = read_to_int(fd, 4)
		num_buckets = read_to_int(fd, 2)
		num_bucket_passed = 0
		hashed = int(md5(value.encode()).hexdigest(), 16) % total_buckets
		if log2(total_buckets) % 1: # should only happend on linear
			level = int(log2(total_buckets))
			split = total_buckets - 2 ** level
			hashbase = 2 ** level
			hashed = int(md5(value.encode()).hexdigest(), 16) % hashbase
			if hashed < split:
				hashed = int(md5(value.encode()).hexdigest(), 16) % (hashbase * 2)
		while overflow:
			if hashed <= num_bucket_passed + num_buckets:
				fd.seek((hashed - num_bucket_passed) * 4, 1)
				break
			else:
				fd.seek(of_pointer * page_size)
				num_bucket_passed += num_buckets
				overflow = read_to_int(fd, 1)
				of_pointer = read_to_int(fd, 3)
				num_buckets = read_to_int(fd, 2)
				page_read += 1
		if not overflow:
			fd.seek(hashed * 4, 1)
		bucket_page = read_to_int(fd, 4)

		overflow = 1
		of_pointer = bucket_page
		while overflow:
			fd.seek(of_pointer * page_size)
			page_read += 1
			overflow = read_to_int(fd, 1)
			of_pointer = read_to_int(fd, 3)
			total_idx = read_to_int(fd, 2)
			num_idx = read_to_int(fd, 2)
			for i in range(num_idx):
				data = fd.read(index_size)
				if data[:-4].decode().strip('\x00') == value:
					rowids.append(int.from_bytes(data[-4:].strip(b'\x00'), byteorder='big'))

	# search for actual data:
	if not rowids:
		print('Value does not exist.')
		return page_read, data_pg_read
	rowids.sort()
	print('find {} results'.format(len(rowids)))
	with open(dbFile, 'rb+') as fd:
		idx = 0
		row_upper = REC_PER_PAGE
		while idx < len(rowids):
			rid = rowids[idx]
			fd.seek(rid * REC_SIZE)
			fn, ln, em = build_record(fd.read(REC_SIZE))
			print('First Name: {}, Last Name: {}, E-mail: {}'.format(fn, ln, em))
			idx += 1
			if rid >= row_upper:
				data_pg_read += 1
				pg_num = rid // REC_PER_PAGE
				row_upper = REC_PER_PAGE * pg_num

	return page_read, data_pg_read

if __name__ == '__main__':
	dbFile = './names.db'
	field = 0
	value = 'Nona'
	idx_on = {0: 'static', 1: 'extend', 2: 'linear'}
	for idx in range(3):
		print('{} hashing'.format(idx_on[idx]))
		indexFile = './index_{}.db'.format(idx_on[idx])
		idx_pg_read, data_pg_read = query(dbFile, indexFile, field, value)
		print('index page read: {}, write: {}; data page read: {}, write {}'\
			.format(idx_pg_read, 0, data_pg_read, 0))
