import time
import util

# counter
header_page_read = 0
data_page_read = 0
table_interior_page_read = 0

def read_table_leaf_cell(fd, usable_size, cell):
	fd.seek(cell)
	payload_byte = util.decode_varint(fd)
	rowid_byte = util.decode_varint(fd)
	header_size = util.decode_varint(fd)[0]
	_, initial_payload_size = util.overflow(13, payload_byte[0], usable_size)
	fd.seek(cell + payload_byte[1] + rowid_byte[1] + header_size)
	emp_id = util.read_to_int(fd, 3)
	reset_of_record = fd.read(initial_payload_size - 3)
	# if overflow:
		# TODO
		# pass
	employee = util.construct_record(emp_id, reset_of_record)
	return employee


def search_table_child(fd, page_size, pg_num, usable_size, target_field, target_value):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]
	if pg_type == 5:
		global table_interior_page_read
		table_interior_page_read += 1
		for cell in cell_pointer:
			fd.seek(cell)
			left_pointer = util.read_to_int(fd, 4)
			employee = search_table_child(fd, page_size, left_pointer, usable_size, target_field, target_value)
			if employee:
				return employee
		return search_table_child(fd, page_size, right_most_pointer, usable_size, target_field, target_value)
	elif pg_type == 13:
		global data_page_read
		data_page_read += 1
		for cell in cell_pointer:
			employee = read_table_leaf_cell(fd, usable_size, cell)
			if employee[target_field] == target_value:
				return employee
		return None
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def no_index_scan(db):
	with open(db, "rb") as fd:
		# read db root
		page_size, usable_size, free_list_page, num_free_list_page = util.read_db_header(fd)
		fd.seek(100)
		_, cell_pointer, _ = util.read_page_header(fd)
		# read sqlite_master
		# assume sqlite_master does not util.overflow
		util.master_move_fd_to_record(fd, cell_pointer[0])
		# 21 is size of 'TableEmployeeEmployee' NOTE: hardcode for now
		fd.seek(21, 1)
		table_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# scan
		print('Scan Last name Rowe')
		employee = search_table_child(fd, page_size, table_root_page, usable_size, 'Last_Name', 'Rowe')
		if employee:
			print('EMP_ID: {}, Name: {} {} {}'.format(employee['EMP_ID'], \
				employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
		else:
			print('No one has last name Rowe')


def no_index_eq(db):
	with open(db, "rb") as fd:
		# read db root
		page_size, usable_size, free_list_page, num_free_list_page = util.read_db_header(fd)
		fd.seek(100)
		_, cell_pointer, _ = util.read_page_header(fd)
		# read sqlite_master
		# assume sqlite_master does not util.overflow
		util.master_move_fd_to_record(fd, cell_pointer[0])
		# 21 is size of 'TableEmployeeEmployee' NOTE: hardcode for now
		fd.seek(21, 1)
		table_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# equality search
		print('Equality employee id 181162')
		employee = search_table_child(fd, page_size, table_root_page, usable_size, 'EMP_ID', 181162)
		if employee:
			print('Name: {} {} {}'.format(employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
		else:
			print('employee id 181162 not exist')


def no_index_range(db):
	with open(db, "rb") as fd:
		# read db root
		page_size, usable_size, free_list_page, num_free_list_page = util.read_db_header(fd)
		fd.seek(100)
		_, cell_pointer, _ = util.read_page_header(fd)
		# read sqlite_master
		# assume sqlite_master does not util.overflow
		util.master_move_fd_to_record(fd, cell_pointer[0])
		# 21 is size of 'TableEmployeeEmployee' NOTE: hardcode for now
		fd.seek(21, 1)
		table_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# range search
		print('Range seach eid between 171800 and 171899')
		for i in range(100):
			eid = 171800 + i
			employee = search_table_child(fd, page_size, table_root_page, usable_size, 'EMP_ID', eid)
			if employee:
				print('EMP_ID: {}, Name: {} {} {}'.format(employee['EMP_ID'], \
					employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
			else:
				print('employee id {} not exist'.format(eid))


if __name__ == '__main__':
	print('Database 1 -- Scan')
	start_time = time.time()
	no_index_scan("./emp1.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 1 -- Equality')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	start_time = time.time()
	no_index_eq("./emp1.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 1 -- Range')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	start_time = time.time()
	no_index_range("./emp1.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))
	
	###############################################

	print('===========================')

	print('Database 2 -- Scan')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	start_time = time.time()
	no_index_scan("./emp2.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 2 -- Equality')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	start_time = time.time()
	no_index_eq("./emp2.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 2 -- Range')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	start_time = time.time()
	no_index_range("./emp2.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))