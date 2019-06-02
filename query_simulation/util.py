# some helper functions

def decode_varint(fd):
	read = 0
	for i in range(9):
		curr = read_to_int(fd, 1)
		if i == 8:
			read += curr
		else:
			read += curr % 128
		if curr < 128: # 8th bit is zero
			break

		# curr >= 128
		if i == 8:
			break
		if i == 7:
			read = read << 8
		else:
			read = read << 7
	return (read, i+1)


def overflow(pg_type, P, U):
	X = U - 35
	if pg_type in [2, 10]:
		X = ((U - 12) * 64 / 255) - 23
	M = ((U - 12) * 32 / 255) - 23
	K = M + ((P - M) % (U - 4))
	overflow = True
	initial_payload_size = P
	if P <= X:
		overflow = False
	elif K <= X:
		initial_payload_size = K
	else:
		initial_payload_size = M
	return overflow, initial_payload_size


def read_to_int(fd, size):
	return int.from_bytes(fd.read(size), byteorder='big')


def read_db_header(fd):
	fd.seek(16)
	page_size = read_to_int(fd, 2)
	fd.seek(2, 1)
	reserved_size = read_to_int(fd, 1)
	usable_size = page_size - reserved_size
	fd.seek(11, 1)
	free_list_page = read_to_int(fd, 4)
	num_free_list_page = read_to_int(fd, 4)
	return page_size, usable_size, free_list_page, num_free_list_page


def read_page_header(fd):
	pg_type = read_to_int(fd, 1)
	free_block = read_to_int(fd, 2) # not used
	num_cell = read_to_int(fd, 2)
	fd.seek(3, 1)
	right_most_pointer = None
	if pg_type in [2, 5]:
		right_most_pointer = read_to_int(fd, 4)
	# read cell pointer array
	cell_pointer = []
	for _ in range(num_cell):
		cell_pointer.append(read_to_int(fd, 2))
	return pg_type, cell_pointer, right_most_pointer


def master_move_fd_to_record(fd, cell):
	fd.seek(cell)
	payload_byte = decode_varint(fd)
	rowid_byte = decode_varint(fd)
	# read row
	header_size = decode_varint(fd)[0]
	fd.seek(cell + payload_byte[1] + rowid_byte[1] + header_size)


def construct_record(emp_id, reset_of_record):
	employee = {}
	employee['EMP_ID'] = emp_id
	record = reset_of_record.decode(errors='ignore')
	employee['First_Name'] = record[5:16].strip()
	employee['Middle_Name'] = record[16]
	employee['Last_Name'] = record[17:30].strip()
	return employee
