import time
import util

# counter
header_page_read = 0
data_page_read = 0
table_interior_page_read = 0
index_leaf_page_read = 0
index_interior_page_read = 0

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

		
def binary_search_index_leaf(fd, cell_pointer, target_value):
	global index_leaf_page_read
	index_leaf_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	payload_byte = util.decode_varint(fd)
	header_size = util.decode_varint(fd)[0]
	emp_id_size = util.read_to_int(fd, 1)
	row_id_size = util.read_to_int(fd, 1)
	fd.seek(cell + payload_byte[1] + header_size)
	emp_id = util.read_to_int(fd, emp_id_size)
	row_id = util.read_to_int(fd, row_id_size)

	if n == 0:
		if target_value == emp_id:
			return row_id
		else:
			return None
	else:
		if target_value < emp_id:
			return binary_search_index_leaf(fd, cell_pointer[:n], target_value)
		else:
			return binary_search_index_leaf(fd, cell_pointer[n:], target_value)


def binary_search_index_interior(fd, cell_pointer, target_value, right_most_pointer):
	global index_interior_page_read
	index_interior_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	left_pointer = util.read_to_int(fd, 4)
	key_payload_byte = util.decode_varint(fd)
	key_header_size = util.decode_varint(fd)[0]
	emp_id_size = util.read_to_int(fd, 1)
	fd.seek(cell + 4 + key_payload_byte[1] + key_header_size)
	emp_id = util.read_to_int(fd, emp_id_size)

	if len(cell_pointer) == 1:
		if target_value < emp_id:
			return left_pointer
		else:
			return right_most_pointer
	elif len(cell_pointer) == 2:
		if target_value < emp_id:
			cell_0 = cell_pointer[0]
			fd.seek(cell_0)
			left_pointer_0 = util.read_to_int(fd, 4)
			key_payload_byte_0 = util.decode_varint(fd)
			key_header_size_0 = util.decode_varint(fd)[0]
			emp_id_size_0 = util.read_to_int(fd, 1)
			fd.seek(cell_0 + key_payload_byte_0[1] + key_header_size_0)
			emp_id_0 = util.read_to_int(fd, emp_id_size_0)
			if target_value < emp_id_0:
				return left_pointer_0
			return left_pointer
		else:
			return right_most_pointer
	else:
		if target_value < emp_id:
			return binary_search_index_interior(fd, cell_pointer[:n+1], target_value, right_most_pointer)
		else:
			return binary_search_index_interior(fd, cell_pointer[n+1:], target_value, right_most_pointer)


def search_index_child(fd, page_size, pg_num, target_value):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]

	if pg_type == 2:
		next_pointer = binary_search_index_interior(fd, cell_pointer, target_value, right_most_pointer)
		return search_index_child(fd, page_size, next_pointer, target_value)
	elif pg_type == 10:
		return binary_search_index_leaf(fd, cell_pointer, target_value)
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def binary_search_table_leaf(fd, cell_pointer, target_value):
	global data_page_read
	data_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	payload_byte = util.decode_varint(fd)
	row_id, row_id_size = util.decode_varint(fd)

	if n == 0:
		if target_value == row_id:
			header_size = util.decode_varint(fd)[0]
			fd.seek(cell + payload_byte[1] + row_id_size + header_size)
			emp_id = util.read_to_int(fd, 3)
			record = fd.read(payload_byte[0] - 3)
			employee = util.construct_record(emp_id, record)
			return employee
		else:
			print('row id does not exist in this page, something is wrong.')
			exit(1)
	else:
		if target_value < row_id:
			return binary_search_table_leaf(fd, cell_pointer[:n], target_value)
		else:
			return binary_search_table_leaf(fd, cell_pointer[n:], target_value)


def binary_search_table_interior(fd, cell_pointer, target_value, right_most_pointer):
	global table_interior_page_read
	table_interior_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	left_pointer = util.read_to_int(fd, 4)
	row_id = util.decode_varint(fd)[0]

	if len(cell_pointer) == 1:
		if target_value <= row_id:
			return left_pointer
		else:
			return right_most_pointer
	if len(cell_pointer) == 2:
		if target_value <= row_id:
			cell_0 = cell_pointer[0]
			fd.seek(cell_0)
			left_pointer_0 = util.read_to_int(fd, 4)
			row_id_0 = util.decode_varint(fd)[0]
			if target_value <= row_id_0:
				return left_pointer_0
			return left_pointer
		else:
			return right_most_pointer
	else:
		if target_value <= row_id:
			return binary_search_table_interior(fd, cell_pointer[:n+1], target_value, right_most_pointer)
		else:
			return binary_search_table_interior(fd, cell_pointer[n+1:], target_value, right_most_pointer)


def search_equality_index(fd, page_size, pg_num, target_value):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]
	if pg_type == 5:
		next_pointer = binary_search_table_interior(fd, cell_pointer, target_value, right_most_pointer)
		return search_equality_index(fd, page_size, next_pointer, target_value)
	elif pg_type == 13:
		return binary_search_table_leaf(fd, cell_pointer, target_value)
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def search_index_leaf_range(fd, cell_pointer, lower_bound, upper_bound):
	global index_leaf_page_read
	index_leaf_page_read += 1
	result = []
	for cell in cell_pointer:
		fd.seek(cell)
		payload_byte = util.decode_varint(fd)
		header_size = util.decode_varint(fd)[0]
		emp_id_size = util.read_to_int(fd, 1)
		row_id_size = util.read_to_int(fd, 1)
		fd.seek(cell + payload_byte[1] + header_size)
		emp_id = util.read_to_int(fd, emp_id_size)
		row_id = util.read_to_int(fd, row_id_size)

		if emp_id > lower_bound and emp_id < upper_bound:
			result.append(row_id)
	return result


def search_index_interior_range(fd, cell_pointer, lower_bound, upper_bound, right_most_pointer):
	global index_interior_page_read
	index_interior_page_read += 1
	result = []
	for cell in cell_pointer:
		fd.seek(cell)
		left_pointer = util.read_to_int(fd, 4)
		key_payload_byte = util.decode_varint(fd)
		key_header_size = util.decode_varint(fd)[0]
		emp_id_size = util.read_to_int(fd, 1)
		fd.seek(cell + 4 + key_payload_byte[1] + key_header_size)
		emp_id = util.read_to_int(fd, emp_id_size)

		if emp_id > lower_bound and emp_id < upper_bound:
			result.append(left_pointer)
		if emp_id > upper_bound:
			result.append(left_pointer)
			break
	if emp_id < upper_bound:
		result.append(right_most_pointer)
	return result


def search_range_index(fd, page_size, pg_num, lower_bound, upper_bound):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]
	if pg_type == 2:
		next_pointers = search_index_interior_range(fd, cell_pointer, lower_bound, upper_bound, right_most_pointer)
		result = []
		for pointer in next_pointers:
			result.extend(search_range_index(fd, page_size, pointer, lower_bound, upper_bound))
		return result
	elif pg_type == 10:
		return search_index_leaf_range(fd, cell_pointer, lower_bound, upper_bound)
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def with_index_with_rowid_scan(db):
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


def with_index_with_rowid_eq(db):
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
		util.master_move_fd_to_record(fd, cell_pointer[1])
		# 40 is size of 'indexsqlite_autoindex_Employee_1Employee' NOTE: hardcode for now
		fd.seek(40, 1)
		index_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# equality search
		print('Equality employee id 181162')
		row_id = search_index_child(fd, page_size, index_root_page, 181162)
		if row_id:
			employee = search_equality_index(fd, page_size, table_root_page, row_id)
			print('Name: {} {} {}'.format(employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
		else:
			print('employee id 181162 not exist')


def with_index_with_rowid_range(db):
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
		util.master_move_fd_to_record(fd, cell_pointer[1])
		# 40 is size of 'indexsqlite_autoindex_Employee_1Employee' NOTE: hardcode for now
		fd.seek(40, 1)
		index_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# range search
		print('Range seach eid between 171800 and 171899')
		row_ids = search_range_index(fd, page_size, index_root_page, 171800, 171899)
		for row_id in row_ids:
			employee = search_equality_index(fd, page_size, table_root_page, row_id)
			print('EMP_ID: {}, Name: {} {} {}'.format(employee['EMP_ID'], \
				employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))


if __name__ == '__main__':
	print('===========================')
	print('Database 3 -- Scan')
	start_time = time.time()
	with_index_with_rowid_scan("./emp3.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read + \
		index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 3 -- Equality')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	index_interior_page_read = 0
	index_leaf_page_read = 0
	start_time = time.time()
	with_index_with_rowid_eq("./emp3.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read + \
		index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 3 -- Range')
	header_page_read = 0
	data_page_read = 0
	table_interior_page_read = 0
	index_interior_page_read = 0
	index_leaf_page_read = 0
	start_time = time.time()
	with_index_with_rowid_range("./emp3.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + data_page_read + table_interior_page_read + \
		index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("data_page_read: {}".format(data_page_read))
	print("table_interior_page_read: {}".format(table_interior_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))