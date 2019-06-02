import sqlite3
import csv
import time

# A part of schema which is common in all four tables
SCHEMA_PART = "Name_Prefix CHAR(5), First_Name CHAR(11), \
	Middle_Name CHAR(1), Last_Name CHAR(13), Gender CHAR(1), E_Mail CHAR(37), Father_Name CHAR(25), \
	Mother_Name CHAR(24), Mother_Maiden_Name CHAR(13), Date_of_Birth CHAR(10), Time_of_Birth CHAR(20), \
	Age CHAR(5), Wight CHAR(2), Date_Join CHAR(10), Quarter_Join CHAR(2), Half_Join CHAR(2), Year_Join CHAR(4), \
	Month_Join CHAR(2), Month CHAR(9), Month_Short CHAR(3), Day_Join CHAR(2), Day CHAR(9), Day_Short CHAR(3), \
	Age_in_Comp CHAR(5), Salary CHAR(6), Last_Hike CHAR(4), SSN CHAR(11), Phone CHAR(12), Place CHAR(26), \
	County CHAR(22), City CHAR(26), State CHAR(2), ZIP CHAR(5), Region CHAR(9), Username CHAR(15), \
	Password CHAR(15))"
SCHEMA_NO_IDX = "CREATE TABLE Employee( EMP_ID INT, " + SCHEMA_PART
SCHEMA_IDX = "CREATE TABLE Employee( EMP_ID INT PRIMARY KEY, " + SCHEMA_PART

DATA_FILE = './500000 Records.csv'
PROCESSED = './processed.csv'
# A list of all char length for each attributes except EMP_ID
CHAR_SIZE = [5, 11, 1, 13, 1, 37, 25, 24, 13, 10, 20, \
	5, 2, 10, 2, 2, 4, 4, 9, 3, 2, 9, 3, 5, 6, 4, 11, 12, 26, 22, 26, 2, 5, 9, 15, 15]


# pad chars to fixed length
def padding(row):
	result = [row[0]]
	for idx, char in enumerate(row[1:]):
		pad_size = CHAR_SIZE[idx] - len(char)
		result.append(pad_size * ' ' + char)
	return tuple(result)


# read and parse data
def preprocess():
	data = []
	seen = []
	with open(DATA_FILE) as csvfile:
		reader = csv.reader(csvfile)
		header = True
		for row in reader:
			if header:
				header = False
				continue
			eid = int(row[0])
			if eid in seen:
				continue
			data.append(padding(row))
			seen.append(eid)
	return data


def save_processed(data):
	with open(PROCESSED, 'w', newline='') as csvfile:
		writer = csv.writer(csvfile, delimiter=' ')
		writer.writerows(data)
	print("file saved.")

def read_processed():
	data = []
	with open(PROCESSED, newline='') as csvfile:
		reader = csv.reader(csvfile, delimiter=' ')
		for row in reader:
			data.append(row)
	return data


def load():
	# start_time = time.time()
	data = preprocess()
	# print("--- %s seconds ---" % (time.time() - start_time))
	# print("finish preprocessing.")
	save_processed(data)
	# data = read_processed()

	# start_time = time.time()
	# print("database operation")

	# setup databases
	conn1 = sqlite3.connect('./emp1.db')
	cursor1 = conn1.cursor()
	cursor1.execute("DROP TABLE IF EXISTS Employee")
	cursor1.execute(SCHEMA_NO_IDX)

	conn2 = sqlite3.connect('./emp2.db')
	cursor2 = conn2.cursor()
	cursor2.execute("PRAGMA page_size = 16384")
	cursor2.execute("DROP TABLE IF EXISTS Employee")
	cursor2.execute(SCHEMA_NO_IDX)

	conn3 = sqlite3.connect('./emp3.db')
	cursor3 = conn3.cursor()
	cursor3.execute("DROP TABLE IF EXISTS Employee")
	cursor3.execute(SCHEMA_IDX)

	conn4 = sqlite3.connect('./emp4.db')
	cursor4 = conn4.cursor()
	cursor4.execute("DROP TABLE IF EXISTS Employee")
	cursor4.execute(SCHEMA_IDX + "WITHOUT ROWID")
	
	conns = [conn1, conn2, conn3, conn4]
	for c in conns:
		c.cursor().executemany("INSERT INTO Employee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
		c.commit()
	# print("--- %s seconds ---" % (time.time() - start_time))
	return conns


if __name__ == '__main__':
	# test
	conns = load()
	for c in conns:
		result = c.cursor().execute("SELECT COUNT(*) FROM Employee")
		for row in result:
			print(row)