import time
import util

# counter
header_page_read = 0
index_leaf_page_read = 0
index_interior_page_read = 0


def scan_index_no_row_id(fd, page_size, pg_num, target_field, target_value):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]
	if pg_type == 2:
		global index_interior_page_read
		index_interior_page_read += 1
		for cell in cell_pointer:
			fd.seek(cell)
			left_pointer = util.read_to_int(fd, 4)
			employee = scan_index_no_row_id(fd, page_size, left_pointer, target_field, target_value)
			if employee:
				return employee
			fd.seek(cell)
			left_pointer = util.read_to_int(fd, 4)
			payload_byte = util.decode_varint(fd)
			header_size = util.decode_varint(fd)[0]
			fd.seek(cell + 4 + payload_byte[1] + header_size)
			emp_id = util.read_to_int(fd, 3)
			reset_of_record = fd.read(payload_byte[0] - 3)
			employee = util.construct_record(emp_id, reset_of_record)
			if employee[target_field] == target_value:
				return employee
		return scan_index_no_row_id(fd, page_size, right_most_pointer, target_field, target_value)
	elif pg_type == 10:
		global index_leaf_page_read
		index_leaf_page_read += 1
		for cell in cell_pointer:
			fd.seek(cell)
			payload_byte = util.decode_varint(fd)
			header_size = util.decode_varint(fd)[0]
			fd.seek(cell + payload_byte[1] + header_size)
			emp_id = util.read_to_int(fd, 3)
			reset_of_record = fd.read(payload_byte[0] - 3)
			employee = util.construct_record(emp_id, reset_of_record)
			if employee[target_field] == target_value:
				return employee
		return None
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def binary_search_index_leaf_no_rowid(fd, cell_pointer, target_value):
	global index_leaf_page_read
	index_leaf_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	payload_byte = util.decode_varint(fd)
	header_size = util.decode_varint(fd)[0]
	fd.seek(cell + payload_byte[1] + header_size)
	emp_id = util.read_to_int(fd, 3)

	if target_value == emp_id:
		reset_of_record = fd.read(payload_byte[0] - 3)
		employee = util.construct_record(emp_id, reset_of_record)
		return employee
	elif n == 0:
		return None
	else:
		if target_value < emp_id:
			return binary_search_index_leaf_no_rowid(fd, cell_pointer[:n], target_value)
		else:
			return binary_search_index_leaf_no_rowid(fd, cell_pointer[n:], target_value)


def binary_search_index_interior_no_rowid(fd, cell_pointer, target_value, right_most_pointer):
	global index_interior_page_read
	index_interior_page_read += 1
	n = len(cell_pointer)//2
	cell = cell_pointer[n]
	fd.seek(cell)
	left_pointer = util.read_to_int(fd, 4)
	payload_byte = util.decode_varint(fd)
	header_size = util.decode_varint(fd)[0]
	fd.seek(cell + 4 + payload_byte[1] + header_size)
	emp_id = util.read_to_int(fd, 3)

	if target_value == emp_id:
		reset_of_record = fd.read(payload_byte[0] - 3)
		employee = util.construct_record(emp_id, reset_of_record)
		return (True, employee)

	if len(cell_pointer) == 1:
		if target_value < emp_id:
			return (False, left_pointer)
		else:
			return (False, right_most_pointer)
	elif len(cell_pointer) == 2:
		if target_value < emp_id:
			cell_0 = cell_pointer[0]
			fd.seek(cell_0)
			left_pointer_0 = util.read_to_int(fd, 4)
			payload_byte_0 = util.decode_varint(fd)
			header_size_0 = util.decode_varint(fd)[0]
			fd.seek(cell_0 + payload_byte_0[1] + header_size_0)
			emp_id_0 = util.read_to_int(fd, 3)

			if target_value < emp_id_0:
				return (False, left_pointer_0)
			return (False, left_pointer)
		else:
			return (False, right_most_pointer)
	else:
		if target_value < emp_id:
			return binary_search_index_interior_no_rowid(fd, cell_pointer[:n+1], target_value, right_most_pointer)
		else:
			return binary_search_index_interior_no_rowid(fd, cell_pointer[n+1:], target_value, right_most_pointer)


def search_index_no_row_id(fd, page_size, pg_num, target_value):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]

	if pg_type == 2:
		flag, value = binary_search_index_interior_no_rowid(fd, cell_pointer, target_value, right_most_pointer)
		if flag:
			return value # found employee
		else:
			return search_index_no_row_id(fd, page_size, value, target_value)
	elif pg_type == 10:
		return binary_search_index_leaf_no_rowid(fd, cell_pointer, target_value)
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)



def search_index_leaf_range_no_rowid(fd, cell_pointer, lower_bound, upper_bound):
	global index_leaf_page_read
	index_leaf_page_read += 1
	result = []
	for cell in cell_pointer:
		fd.seek(cell)
		payload_byte = util.decode_varint(fd)
		header_size = util.decode_varint(fd)[0]
		fd.seek(cell + payload_byte[1] + header_size)
		emp_id = util.read_to_int(fd, 3)

		if emp_id > lower_bound and emp_id < upper_bound:
			reset_of_record = fd.read(payload_byte[0] - 3)
			employee = util.construct_record(emp_id, reset_of_record)
			result.append(employee)
	return result


def search_index_interior_range_no_rowid(fd, cell_pointer, lower_bound, upper_bound, right_most_pointer):
	global index_interior_page_read
	index_interior_page_read += 1
	pointers = []
	employees = []
	for cell in cell_pointer:
		fd.seek(cell)
		left_pointer = util.read_to_int(fd, 4)
		payload_byte = util.decode_varint(fd)
		header_size = util.decode_varint(fd)[0]
		fd.seek(cell + 4 + payload_byte[1] + header_size)
		emp_id = util.read_to_int(fd, 3)

		if emp_id > lower_bound and emp_id < upper_bound:
			reset_of_record = fd.read(payload_byte[0] - 3)
			employee = util.construct_record(emp_id, reset_of_record)
			employees.append(employee)
			pointers.append(left_pointer)
		if emp_id > upper_bound:
			pointers.append(left_pointer)
			break
	if emp_id < upper_bound:
		pointers.append(right_most_pointer)
	return pointers, employees


def search_range_index_no_rowid(fd, page_size, pg_num, lower_bound, upper_bound):
	page_init = page_size * (pg_num - 1)
	fd.seek(page_init)
	pg_type, cell_pointer, right_most_pointer = util.read_page_header(fd)
	cell_pointer = [cell + page_init for cell in cell_pointer]
	if pg_type == 2:
		next_pointers, employees = search_index_interior_range_no_rowid(fd, cell_pointer, lower_bound, upper_bound, right_most_pointer)
		for pointer in next_pointers:
			employees.extend(search_range_index_no_rowid(fd, page_size, pointer, lower_bound, upper_bound))
		return employees
	elif pg_type == 10:
		return search_index_leaf_range_no_rowid(fd, cell_pointer, lower_bound, upper_bound)
	else:
		print('Wrong page type at {}'.format(page_init))
		exit(1)


def with_index_no_rowid_scan(db):
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
		index_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# scan
		print('Scan Last name Rowe')
		employee = scan_index_no_row_id(fd, page_size, index_root_page, 'Last_Name', 'Rowe')
		if employee:
			print('EMP_ID: {}, Name: {} {} {}'.format(employee['EMP_ID'], \
				employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
		else:
			print('No one has last name Rowe')
		

def with_index_no_rowid_eq(db):
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
		index_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1

		# equality search
		print('Equality employee id 181162')
		employee = search_index_no_row_id(fd, page_size, index_root_page, 181162)
		if employee:
			print('Name: {} {} {}'.format(employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))
		else:
			print('employee id 181162 not exist')


def with_index_no_rowid_range(db):
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
		index_root_page = util.read_to_int(fd, 1)
		global header_page_read
		header_page_read += 1
		
		# range search
		print('Range seach eid between 171800 and 171899')
		employees = search_range_index_no_rowid(fd, page_size, index_root_page, 171800, 171899)
		employees.sort(key=lambda employee: employee['EMP_ID'])
		for employee in employees:
			print('EMP_ID: {}, Name: {} {} {}'.format(employee['EMP_ID'], \
				employee['First_Name'], employee['Middle_Name'], employee['Last_Name']))


if __name__ == '__main__':
	print('===========================')
	print('Database 4 -- Scan')
	start_time = time.time()
	with_index_no_rowid_scan("./emp4.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 4 -- Equality')
	header_page_read = 0
	index_interior_page_read = 0
	index_leaf_page_read = 0
	start_time = time.time()
	with_index_no_rowid_eq("./emp4.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))

	print('Database 4 -- Range')
	header_page_read = 0
	index_interior_page_read = 0
	index_leaf_page_read = 0
	start_time = time.time()
	with_index_no_rowid_range("./emp4.db")
	delt_time = time.time() - start_time
	total_pages = header_page_read + index_interior_page_read + index_leaf_page_read
	print("header_page_read: {}".format(header_page_read))
	print("index_interior_page_read: {}".format(index_interior_page_read))
	print("index_leaf_page_read: {}".format(index_leaf_page_read))
	print("average time per page: {}s seconds".format(delt_time/total_pages))
	print("--- %s seconds ---" % (delt_time))