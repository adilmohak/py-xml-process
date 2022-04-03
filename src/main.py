import psycopg2
from settings import DELETION_DATE, DATABASES, XMLFILE, URL, BASE_PICTURES_PATH
from utils import convert_str_to_datetime, get_images_full_path, get_formated_str, get_valid_value, get_formated_str
import logging
import requests
import os
import datetime
import xml.etree.ElementTree as ET
from pathlib import Path


class FileManager:
	def __init__(self):
		self.file = XMLFILE
	
	def rename_file(self):
		now = datetime.datetime.now()
		try:
			os.rename ("epg.xml", f"epg_{now.year}-{now.month}-{now.day}.xml")
		except:
			pass

	def download_file(self):
		try:
			response = requests.get(URL)
			if response.status_code == 200:
				with open(self.file, 'wb') as file:
					file.write(response.content)
			else:
				raise SystemExit(":( Something went wrong while downloading")
		except requests.exceptions.RequestException as e:  # catch any error
			raise SystemExit(e)

	def clean_old_pictures_from_directory():
		"""
			Delete old pictures in the directories
		"""
		today = datetime.today()#gets current time
		os.chdir(BASE_PICTURES_PATH) # changing path to current path(same as cd command)

		count = 0
		# we are taking current folder, directory and files 
		# separetly using os.walk function
		for root, dirs, files in os.walk(BASE_PICTURES_PATH,topdown=False): 
			for file in files:
				# Get the last modified time
				file_timestamp = os.stat(os.path.join(root, file))[8] 

				filetime_obj = datetime.fromtimestamp(file_timestamp)	
				filetime = filetime_obj - today		
				if filetime_obj <= datetime.now() - datetime.timedelta(days=DELETION_DATE):
					os.remove(os.path.join(root, file))
					count += 1

		logging.info(f"{count} old images removed!\n")


sql_commands = {
	"create_table": """
		CREATE TABLE epg.%s (
            id SERIAL PRIMARY KEY,
			channel_id CHARACTER VARYING(200),
			start TIMESTAMP WITH TIME ZONE,
			stop TIMESTAMP WITH TIME ZONE,
			title VARCHAR(220),
			sub_title VARCHAR(220),
			descriptions TEXT [],
			directors TEXT [],
			actors TEXT [],
			categories TEXT [],
			icons TEXT [],
			countries TEXT [],
			stereo VARCHAR(120),
			csfd_id INT,
			marked_start BOOL DEFAULT 'f',
			marked_stop BOOL DEFAULT 'f',
			epg_verified BOOL DEFAULT 'f',
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
			UNIQUE (title, start, stop),
			FOREIGN KEY (channel_id)
                    REFERENCES public.countries_countries (pressdata_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
		""",
	"insert": """
		INSERT INTO epg.{} (
			channel_id, start, stop, title, sub_title, descriptions, categories, 
			icons, countries, stereo, csfd_id, actors, directors, timestamp
			) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		""",
	"delete": """
			DELETE FROM epg.%s
			WHERE timestamp < now() - interval '%s days'
		""",
	"select_all_tables": """
			SELECT table_name
			FROM information_schema.tables
			WHERE (table_schema = 'epg') AND (table_name LIKE 'pressdata_%')
			ORDER BY table_name;
		""",
	"select_countries": """
			SELECT radioname, pressdata_id 
			FROM public.countries_countries  
			WHERE pressdata_id IS NOT NULL
		""",
	"update": """
			UPDATE epg.{} SET 
			descriptions = %(des)s, categories = %(cat)s, icons = %(ico)s, countries = %(cou)s, stereo = %(ste)s, csfd_id = %(csf)s, actors = %(act)s, directors = %(dir)s, timestamp = %(tim)s
			WHERE (title = %(tit)s AND start = %(sta)s AND stop = %(sto)s)
			AND (sub_title {} OR descriptions != %(des)s OR categories != %(cat)s OR icons != %(ico)s OR countries != %(cou)s OR stereo {} OR csfd_id {} OR actors {} OR directors {});
		"""
}


class DataManager:
	def __init__(self, db):
		try:
			self.connection = psycopg2.connect(database=db['DATABASE'], user=db['USER'], password=db['PASSWORD'], host=db['HOST'], port=db['PORT'])
			logging.info("Database connected.")
			self.connection.autocommit = True
			self.cursor = self.connection.cursor()
			self.row_imported = 0
			self.row_updated = 0
			self.row_deleted = 0
		except Exception as e:
			raise e

	def create_table(self, t_name):
		try:
			# query = self.cursor.mogrify(sql_commands['create_table'] % (t_name))
			# self.cursor.execute(query)
			self.cursor.execute(sql_commands['create_table'] % t_name)
		except (Exception, psycopg2.DatabaseError) as e:
			logging.error(e)

	def delete_old_data(self):
		self.cursor.execute(sql_commands['select_all_tables'])
		result = self.cursor.fetchall()
		for r in result:
			try:
				self.cursor.execute(sql_commands['delete'] % (r[0], DELETION_DATE))
				self.row_deleted += self.cursor.rowcount
			except (Exception, psycopg2.DatabaseError) as e:
				logging.error(e)

	def get_table_values(self, program, now):
		program_channel = program.attrib['channel']
		start = convert_str_to_datetime(program.attrib['start'])
		stop = convert_str_to_datetime(program.attrib['stop'])
		title = program.find('title').text
		countries = [country.text for country in program.findall('country')]
		descriptions = [dec.text for dec in program.findall('desc')]
		categories = [cat.text for cat in program.findall('category')]
		icons = get_images_full_path(program)
		try:
			sub_title = program.find('sub-title').text
		except:
			sub_title = None
		try:
			audio = program.find('audio')
			stereo = audio.find('stereo').text
		except:
			stereo = None
		try:
			csfd_id = int(program.find('csfd-id').text)
		except:
			csfd_id = None
		credits = program.find('credits')
		try:
			actors = [actor.text for actor in credits.findall('actor')]
		except:
			actors = None
		try:
			directors = [director.text for director in credits.findall('director')]
		except:
			directors = None
		
		context = {
			'pro': program_channel,
			'sta': start,
			'sto': stop,
			'tit': title,
			'sub': sub_title,
			'des': descriptions,
			'cat': categories,
			'ico': icons,
			'cou': countries,
			'ste': stereo,
			'csf': csfd_id,
			'act': actors,
			'dir': directors,
			'tim': now
		}
		return (context)

	def update_data(self, table_name, values_):
		formated_query = sql_commands['update'].format(
			table_name, 
			get_valid_value('sub', values_['sub']), 
			get_valid_value('ste', values_['ste']), 
			get_valid_value('csf', values_['csf']), 
			get_valid_value('act', values_['act']), 
			get_valid_value('dir', values_['dir'])
			)
		query = self.cursor.mogrify(formated_query, values_)
		self.cursor.execute(query)
		self.row_updated += self.cursor.rowcount

	def import_data(self):
		tree = ET.parse(XMLFILE)
		root = tree.getroot()
		now = datetime.datetime.now()
		
		self.cursor.execute(sql_commands['select_countries'])
		result = self.cursor.fetchall()
		coutries = []
		for r in result:
			coutries.append(r[1])
		
		created_tables = []
		for program in root.iter('programme'):
			values_ = self.get_table_values(program, now)
			values = tuple([v for k, v in values_.items()])
			table_name = "_%s" % (values_['pro'].replace('.', '_'))
			
			if (values_['pro'] in coutries) and not (values_['sta'] <= now - datetime.timedelta(days=DELETION_DATE)):

				if not (table_name in created_tables):
					self.create_table(table_name)
					created_tables.append(table_name)
				try:
					self.cursor.execute(sql_commands['insert'].format(table_name), (values))
					self.row_imported += self.cursor.rowcount

				except psycopg2.errors.UniqueViolation:
					self.update_data(table_name, values_)

				except (Exception, psycopg2.DatabaseError) as e:
					logging.error(e)

	def result_info(self):
		print(
			"\n===========================\n", 
			self.row_imported, "New rows imported\n", 
			self.row_updated, "Rows updated\n", 
			self.row_deleted, "Old rows deleted\n", 
			"===========================\n"
		)


def main():
	# Data manager, --> rename, download, delete old images
	file_obj = FileManager()
	file_obj.rename_file()
	file_obj.download_file()
	file_obj.clean_old_pictures_from_directory()

	# Data manager, --> Import new data, update if changes, delete old data
	data_obj = DataManager(DATABASES['test'])
	data_obj.delete_old_data()
	data_obj.import_data()
	data_obj.result_info()


if __name__ == '__main__':
	main()
