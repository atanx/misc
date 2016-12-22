#!/usr/bin/python
# coding:utf-8

import os,sys
import Configuration
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
		self.__structure={}

		self.formateArgv()
		self.getMysqlStructure()




	def formateArgv(self):
		today=datetime.date.today() 
		oneday=datetime.timedelta(days=1) 
		data_date=(today-oneday).strftime('%Y%m%d')
		data_date2=(today-oneday).strftime('%Y-%m-%d')

		#db名
		self.__db = ""
		#数据日期
		self.__dt = data_date
		self.__dt_zkb = data_date2
		#table 列表
		self.__tb = []

		if len(sys.argv) < 2:
			sys.exit(1)
		else:
			self.__db = sys.argv[1]
			if len(sys.argv) >2: 
				self.__dt = sys.argv[2]
		
			if len(sys.argv) > 3:
				self.__tb = sys.argv[3].split(",")
		

		db_conf = self.__Conf.get_Conf_Value('db')

		if self.__db not in db_conf:
			print "db不合法"
			sys.exit(1)

		if len(self.__tb) == 0:
			self.__tb = db_conf[self.__db]["db_list"].split(",")

	def printTest(self):
		print self.__db,self.__dt,self.__tb,self.__structure

	def getMysqlStructure(self):
		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/*.bz2"
		if self.__db == "zkb_db":
			hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + "zkb_" + self.__dt_zkb + ".sql.bz2"
		try:
			subprocess.call("hadoop fs -text %s | sed -n '/CREATE TABLE `/,/) ENGINE/p' > %s.sql" % (hdfs_path,self.__db), shell=True)
		except:
			traceback.print_exc()
			exit(1)

		for table in self.__tb:
			table_structure = []
			try:
				context = commands.getoutput("sed -n '/CREATE TABLE `%s`/,/) ENGINE/p' %s.sql" % (table,self.__db))
				for line in context.split('\n')[1:-1] :
					line = line.lstrip()
					if line.startswith('KEY') or line.startswith('PRIMARY KEY') or line.startswith('UNIQUE KEY'):
						continue
					column, type = line.split(' ', 2)[:2]
					column = column[1:-1]
					if column in self.__Keyword:
						column = 'trans_' + column
					type = type.split('(')[0]
					type = type.strip(",")
					table_structure.append((column,type))
			except:
				traceback.print_exc()
				exit(1) 

				table_structure = []
			self.__structure[table] = table_structure

		try:
			subprocess.call("rm -f %s.sql" % self.__db, shell=True)
		except:
			traceback.print_exc()
			exit(1) 

	def createHiveTable(self):
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
	def get_part(self,line, sep):
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

	def subst(self,row, old, new):
		for (i, v) in enumerate(row):
			if v == old:
				row[i] = new
			if v[0] == "'" and v[-1] == "'":
				row[i] = v[1:-1]

	def putHiveData(self):
		
		os.system("> %s_%s.db.info" % (self.__db,self.__dt))
		output = open('%s_%s.db.info' % (self.__db,self.__dt), 'w')

		hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + self.__dt + "/*.bz2"
		if self.__db == "zkb_db":
			hdfs_path = self.__db_conf[self.__db]["hdfs_dir"] + "zkb_" + self.__dt_zkb + ".sql.bz2"
		response = os.popen("hadoop fs -text %s " % (hdfs_path)).readlines()
		for line in response:
			if line.startswith("INSERT"):
				part = line.split('`', 3)
				if len(part)<2:
					continue
				table = part[1]
				if not table in self.__tb:
					continue
				
				for item in self.get_part(line[line.find('(')+1:-1], '('):
					row = self.get_part(item[:-2], ',')
					self.subst(row, 'NULL', '\N')
					tag = self.__db+'#'+self.__dt+'#'+table+'#'
					output.write(tag + "\t" + '\t'.join(row) + "\n")
					#print tag + "\t" + '\t'.join(row)
		output.close()

		for table in self.__tb:
			db_file = "%s_%s.db.info" % (self.__db,self.__dt)
			table_file = "%s_%s_%s.tb.info" % (self.__db,self.__dt,table)
			table_tag = '%s#%s#%s#' % (self.__db,self.__dt,table)
			os.system(" grep '%s' %s|sed -e 's/%s\t//g' > %s " % (table_tag,db_file,table_tag,table_file)  )

			subprocess.call(\
						'hive -S -e "load data local inpath \'%s\' overwrite into table %s.%s;"' \
						% (table_file,self.__db,table),\
						shell=True) 
			os.system("rm -f %s" % table_file)

		os.system("rm -f %s_%s.db.info" % (self.__db,self.__dt))


if __name__ == '__main__':
	job = ImportDB()
	job.createHiveTable()
	job.putHiveData()
