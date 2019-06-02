import argparse
import math
import os
import matplotlib.pyplot as plt

FN_LENGTH = 12
LN_LENGTH = 14
EM_LENGTH = 38
REC_SIZE = FN_LENGTH + LN_LENGTH + EM_LENGTH
TEMP_FILE = './temp.db'


class Ex_sort:
	def __init__(self, inDB, outDB, num_buffer, page_size, field):
		self.inDB = inDB
		self.outDB = outDB
		self.num_in_buffer = num_buffer - 1
		self.page_size = page_size
		self.num_rec = self.page_size // REC_SIZE # number of records per page
		self.field = field
		self.num_page = 0
		self.passes = 0
		self.pg_read = 0
		self.pg_write = 0
	
	# convert a page of binary records into a list of record
	# each record is a tuple
	def build_record(self, record):
		records = []
		for i in range(self.num_rec):
			rec = record[REC_SIZE * i: REC_SIZE * (i + 1)]
			first_name = rec.decode()[:FN_LENGTH].strip('\x00')
			last_name = rec.decode()[FN_LENGTH : -EM_LENGTH].strip('\x00')
			email = rec.decode()[-EM_LENGTH:].strip('\x00')
			if first_name and last_name and email:
				records.append((first_name, last_name, email))
		return records

	# convert a list of records into a binary record page
	def build_record_binary(self, records):
		record = ''
		for r in records:
			record += r[0]
			for _ in range(FN_LENGTH - len(r[0])):
				record += '\x00'
			record += r[1]
			for _ in range(LN_LENGTH - len(r[1])):
				record += '\x00'
			record += r[2]
			for _ in range(EM_LENGTH - len(r[2])):
				record += '\x00'
		if len(record) < self.page_size:
			record += '\x00' * (self.page_size - len(record))
		return record.encode()

	# read and process a page
	def read(self, in_file, page):
		with open(in_file, 'rb+') as fd:
			fd.seek(page * self.page_size)
			record = self.build_record(fd.read(self.page_size))
		self.pg_read += 1
		return record

	def write(self, out_file, page):
		data = self.build_record_binary(page)
		with open(out_file, 'ab+') as fd:
			fd.write(data)
		self.pg_write += 1

	def pass_zero(self):
		open(TEMP_FILE, 'w').close()
		EOF = False
		fd = open(self.inDB, 'rb+')
		while not EOF:
			buff = []
			for _ in range(self.num_in_buffer):
				data = fd.read(self.page_size)
				if not data:
					EOF = True
					break
				buff.append(self.build_record(data))
				self.pg_read += 1
			for b in buff:
				self.write(TEMP_FILE, sorted(b, key=lambda x:x[self.field]))
		fd.close()
		self.num_page = self.pg_read
		self.passes = 1

	def merge(self, pages, in_file, out_file, jump):
		curr_page = pages[:]
		buff = [self.read(in_file, page) for page in pages]
		curr_idx = [0 for _ in pages]
		# number of output buffer == number of input buffer
		# counter = 0
		for _ in range(self.num_in_buffer * jump):
			if not buff:
				break
			output = []
			for _ in range(self.num_rec):
				compare = [b[curr_idx[i]][self.field] for i, b in enumerate(buff)]
				if not compare:
					break
				min_idx = compare.index(min(compare))
				output.append(buff[min_idx][curr_idx[min_idx]])
				curr_idx[min_idx] += 1
				if curr_idx[min_idx] >= self.num_rec:
					next_pg = curr_page[min_idx] + 1
					curr_page[min_idx] = next_pg
					# next page
					if next_pg - pages[min_idx] < jump and next_pg < self.num_page:
						buff[min_idx] = self.read(in_file, next_pg)
						curr_idx[min_idx] = 0
					# all pages in this group has been written to output
					# remove this group from reserve
					else:
						buff = buff[:min_idx] + buff[min_idx + 1:]
						curr_idx = curr_idx[:min_idx] + curr_idx[min_idx + 1:]
						pages = pages[:min_idx] + pages[min_idx + 1:]
						curr_page = curr_page[:min_idx] + curr_page[min_idx + 1:]
			self.write(out_file, output)

	# preform one pass of merge
	def merge_one_pass(self):
		# if numer of passes is odd, read from temparary file and write to output file, vice versa
		in_file = self.outDB
		out_file = TEMP_FILE
		if self.passes % 2:
			in_file = TEMP_FILE
			out_file = self.outDB
		open(out_file, 'w').close()
		
		# self.passes >= 1
		jump = self.num_in_buffer ** (self.passes - 1)
		# a list of starting page for each run
		starts = list(range(0, self.num_page, self.num_in_buffer * jump))
		for curr in starts:
			# find inital starting pages of each sorted group
			end = curr + self.num_in_buffer * jump
			if end > self.num_page:
				end = self.num_page
			pages = list(range(curr, end, jump))

			self.merge(pages, in_file, out_file, jump)
		self.passes += 1

	def ex_sort(self):
		self.pass_zero()
		# self.num_page calculated in pass zero
		num_pass_need = math.ceil(math.log(self.num_page, self.num_in_buffer))
		for _ in range(num_pass_need):
			self.merge_one_pass()
		# make the final version outDB
		if self.passes % 2:
			os.remove(TEMP_FILE)
		else:
			os.remove(self.outDB)
			os.rename(TEMP_FILE, self.outDB)

if __name__ == '__main__':
	# parser = argparse.ArgumentParser(description='external sorting')
	# parser.add_argument('inDB', help='database filename to be sorted')
	# parser.add_argument('outDB', help='sorted database filename')
	# parser.add_argument('num_buffer', type=int, help='number of buffer pages in memory')
	# parser.add_argument('page_size', type=int, help='data page size that is a multiply of 64')
	# parser.add_argument('field', type=int, help='(the index of the field to be sorted by; \
	# 	0=First Name, 1=Last Name, 2=Email')
	# args = parser.parse_args()

	inDB = './names.db'
	outDB = './ex_sort.db'
	field = 1
	for page_size in [512, 1024, 2048]:
		page_rw = []
		buffers = [3, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000]
		for num_buffer in buffers:
			sorter = Ex_sort(inDB, outDB, num_buffer, page_size, field)
			sorter.ex_sort()
			page_rw.append(sorter.pg_read + sorter.pg_write)
			print('page size: {}, buffer size: {}, passes: {}, page read: {}, page write: {}.'\
				.format(page_size, num_buffer, sorter.passes, sorter.pg_read, sorter.pg_write))
		print('-'*70)
		# plot
		plt.figure()
		title = 'page read and write with page size {}'.format(page_size)
		plt.title(title)
		plt.plot([str(b) for b in buffers], page_rw)
		plt.xlabel("buffer size")
		plt.ylabel("page read and write")
		plt.savefig('ex_sort_{}.png'.format(page_size))
