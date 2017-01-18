#!/usr/bin/python
# coding:utf-8

import os,sys
import configuration
import datetime
import traceback
import subprocess
import commands


class ImportDB():
	
	def __init__(self):
		self.__Conf = Configuration.Configuration()
		self.__db_conf = self.__Conf.get_Conf_Value('db')
		self.__Keyword = self.__Conf.get_Conf_Value('core','Hive_Keyword','keyword').strip().split(',')
		self.__Typedict = self.__Conf.get_Conf_Value('core','MysqlToHive_Type')
		self.__db = ""
		self.__dt = ""  
		self.__dt_zkb = ""  
		self.__tb = []
		self.__tb_pref = []
		self.__structure={}

		self.read_options()
		self.load_mysql_schema()

	def read_options(self):
		today=datetime.date.today() 
		oneday=datetime.timedelta(days=1) 
		data_date=(today-oneday).strftime('%Y%m%d')
		self.__dt = data_date

		if len(sys.argv) <= 2:
			sys.exit(1)
		elif len(sys.argv) == 3:
			self.__db = sys.argv[1]
			self.__dt = sys.argv[2]
		else:
			self.__db = sys.argv[1]
			self.__dt = sys.argv[2]
			self.__tb = sys.argv[3].split(",")

		db_conf = self.__Conf.get_Conf_Value('db')

		if self.__db not in db_conf:
			print "db不合法"
			sys.exit(1)

		if len(self.__tb) == 0:
			self.__tb = [table.strip() for table in db_conf[self.__db]["db_list"].split(",")]
			self.__tb_pref = [table.strip() for table in db_conf[self.__db]["tb_pref_list"].split(',') ]

	def print_test(self):
		print self.__db,self.__dt,self.__tb,self.__structure

	def parse_table_fields(self, mysql_table_name):
		lines = commands.getoutput("sed -n '/CREATE TABLE `%s`/,/) ENGINE/p' %s.sql" % (mysql_table_name,self.__db))
		for line in lines:
			line = line.lstrip()
			if line.startswith('KEY') or line.startswith('PRIMARY KEY') or line.startswith('UNIQUE KEY'):
				continue
			field, field_type = line.split(' ',2)[:2]
			if field in self.__Keyword:
				field = 'trans_' + field
			field_type = field_type.split('(')[0]
			field_type = field_type.strip(',')
			fields.append((field, field_type))
			return fields

	def table_grouping(self):
		# 收集call_db中的所有的表名
		context = commands.getoutput("sed -n '/CREATE TABLE `%s`/p' %s.sql" % (table,self.__db))
		groups = dict(zip(self.__tab_pref, [[]] * len(self.__tab_pref)))
		for line in context.split('\n')[1:-1] :
			line = line.lstrip()
			table = re.find('`(.*)`', line)
			for pref in self.__tb_pref:
				if pref in table:
					table_list = groups[pref]
					table_list.append(table)
					groups[pref] = table_list
		return groups
		
	def load_mysql_schema(self):
		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/db_xcall.sql.bz2"
		subprocess.call("hadoop fs -text %s | sed -n '/CREATE TABLE `/,/) ENGINE/p' > %s.sql" % (hdfs_path,self.__db), shell=True)
		groups = self.table_grouping()
		self.__tb_groups = groups
		# 有分表时，仅保存一次
		for group_name in self.__tb_groups:
			tables = self.__tb_groups[group_name]
			if tables:
				table = tables[0]
				fields = self.parse_table_fields(table)
				self.__structure[group_name] = fields
		
		for table in self.__tb:
			fields = []
			fields = parse_table_fields(table)
			self.__structure[table] = fields
			self.__tb_groups = groups
		for table in self.__tb_groups:
		subprocess.call("rm -f %s.sql" % self.__db, shell=True)

	def create_hive_table(self):
		for table in self.__tb:
			mysql_structure = self.__structure[table]
			ddl_cmd = 'use ' + self.__db + '; '
			ddl_cmd = ddl_cmd + 'drop table if exists %s ; ' % (table)
			ddl_cmd = ddl_cmd + 'create table %s ( ' % (table)
			for i in range(len(mysql_structure)) :
				column,ctype = mysql_structure[i]
				ctype = self.__Typedict[ctype]
				ddl_cmd = ddl_cmd + '%s %s,' % (column,ctype)
			ddl_cmd = ddl_cmd.strip(',')
			ddl_cmd = ddl_cmd + ') '
			ddl_cmd = ddl_cmd + ' row format delimited fields terminated by \'\\t\' '
			hive_cmd = 'hive -S -e "%s"' % ddl_cmd
			subprocess.call(hive_cmd, shell=True)

	@staticmethod
	def split_row(line, sep):
		parts = []
		status = 0
		part = ''
		for index in range(0, len(line)):
			ch = line[index]
			if ch == "'":
				i = index
				while i >= 0:
					i -= 1
					if i>=0 and line[i] != '\\':
						break
				if (index - i) % 2 == 1:
					status = 1 - status
			if status == 0 and ch == sep:
				parts.append(part)
				part = ''
			else:
				part += ch
		parts.append(part)
		return parts

	@staticmethod
	def replace(row, old, new):
		for (i, v) in enumerate(row):
			if v == old:
				row[i] = new
			if v[0] == "'" and v[-1] == "'":
				row[i] = v[1:-1]

	def to_hive(self):
		output = open('%s_%s.db.info' % (self.__db,self.__dt), 'w+')
		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/db_xcall.sql.bz2"
		response = os.popen("hadoop fs -text %s " % (hdfs_path)).readlines()
		for line in response:
			parts = line.split('`',2)
			if line.startswith("INSERT") and len(parts)==3:
				table = parts[1]

				need_skip = True
				if table in self.__tb:
					need_skip = False
				for group_name, tables in self.__tb_groups.iteritems():
					if table in tables:
						table = group_name
						need_skip = False
						break
				if need_skip:
					continue

				for item in self.split(line[line.find('(')+1:-1], '('):
					row = self,split_row(item[:-2], ',')
					self.replace(row, 'NULL', '\N')
					tag = self.__db+'#'+self.__dt+'#'+table+'#'
					output.write(tag + "\t" + '\t'.join(row) + "\n")
					#print tag + "\t" + '\t'.join(row)
		output.close()
		
		# 生成hive表数据文件
		selected_tables = self.__tb + self.__tb_group.keys()
		for table in selected_tables:
			db_file = "%s_%s.db.info" % (self.__db,self.__dt)
			table_file = "%s_%s_%s.tb.info" % (self.__db,self.__dt,table)
			table_tag = '%s#%s#%s#' % (self.__db,self.__dt,table)
			os.system(" grep '%s' %s|sed -e 's/%s\t//g' > %s " % (table_tag,db_file,table_tag,table_file)  )

			subprocess.call(\
						'hive -S -e "load data local inpath \'%s\' overwrite into table %s.%s;"' \
						% (table_file,self.__db,table), shell=True) 
			os.system("rm -f %s" % table_file)

		os.system("rm -f %s_%s.db.info" % (self.__db,self.__dt))


if __name__ == '__main__':
	job = ImportDB()
	job.create_hive_table()
	job.to_hive()
