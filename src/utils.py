from datetime import datetime
import re
import psycopg2

from settings import DATABASES, BASE_PICTURES_PATH, PICTURES_DIR
import requests

from pathlib import Path


def connect():
	print("Connecting to the database...")
	database = DATABASES['test']
	conn = psycopg2.connect(
		dbname = database['DATABASE'], 
		user = database['USER'], 
		password = database['PASSWORD'], 
		host = database['HOST'], 
		port = database['PORT']
	)
	print("Connected.")

	return conn


def convert_str_to_datetime(datetime_str):
	datetime_str = datetime_str.split()[0]
	datetime_formated = "{}/{}/{} {}:{}:{}".format(
		datetime_str[0:4], 
		datetime_str[4:6], 
		datetime_str[6:8], 
		datetime_str[8:10], 
		datetime_str[10:12], 
		datetime_str[12:14]
	)
	datetime_obj = datetime.strptime(datetime_formated, '%Y/%m/%d %H:%M:%S')
	# datetime_obj.replace(tzinfo=timezone.utc)
	# print(datetime_obj.timestamp())
	return datetime_obj


def is_downloadable(content_type):
	"""
	Does the url contain a downloadable resource
	"""
	try:
		# h = requests.head(url, allow_redirects=True)
		# header = h.headers
		# content_type = header.get('content-type')
		if 'text' in content_type.lower():
			print("Can't download this image\n")
			return False
		if 'html' in content_type.lower():
			print("Can't download this image\n")
			return False
		return True
	except:
		return False


def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def get_formated_str(base_columns, columns):
	valid_columns = base_columns
	for col in columns:
		if col is not None:
			valid_columns += (col, )
	return tuple(valid_columns)


def return_valid_value(column_name, column_val):
	if column_val == None:
		return f"{column_name} IS NOT NULL"
	else:
		return f"{column_name} != %s"


def get_images_full_path(program):
	pictures = []
	program_channel = program.attrib['channel']
	Path(BASE_PICTURES_PATH / program_channel).mkdir(parents=True, exist_ok=True)
	
	try:
		for pic_url in program.iter('icon'):
			url = pic_url.attrib['src']
			# filename = get_filename(url)
			
			response = requests.get(url, stream=True)
			filename = get_filename_from_cd(response.headers.get('content-disposition'))
			pictures.append(f"{PICTURES_DIR}{program_channel}/" + filename)
	except:
		pass
	return pictures


def get_valid_value(col_name, column_val):
	if column_val == None:
		return f"IS NOT NULL"
	else:
		return f"!= %({col_name})s"


def get_formated_str(base_columns, columns):
	valid_columns = base_columns
	for col in columns:
		if col is not None:
			valid_columns += (col, )
	return tuple(valid_columns)
