#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#*********************************************************************
## Python Database Handler Library
## Mar 25th, 2016
## Author Jonathan Salazar Santos
## E-mail jonanx779@hotmail.com
#*********************************************************************
## 	Description
##
## 		This scripts is intendet to help with the PG database querying
##		throught the psycopg2 library. Trying to separate the database 
##		operations layer with the other scripting clasess to avoid sql
##		mixing and to enfoce coding reutilization and the OOP concept.
##
#*********************************************************************
## 	Version history
##
## 		Version v1.0.0 [Mar 25th, 2016] - Starting point
##
#*********************************************************************

#import os, sys
#import StringIO
import string
#import time
#import random
import psycopg2 as db
import psycopg2.extras

class DbHandler:

	def __init__(self, DATABASE_NAME, DATABASE_USER, DATABASE_PW, DATABASE_HOSTNAME):
		self.__DATABASE_NAME = DATABASE_NAME
		self.__DATABASE_USER = DATABASE_USER
		self.__DATABASE_PW = DATABASE_PW
		self.__DATABASE_HOSTNAME = DATABASE_HOSTNAME
		self.DB_CONNECTION = None

	def db_connection(self):
		"""
		This method sets the object db connection across the whole instances that needs him.

		returns a dict(status = boolean, db_link = db instance)
		"""
		try:
			self.DB_CONNECTION = psycopg2.connect("dbname = " + self.__DATABASE_NAME + " user = " + self.__DATABASE_USER + " host = " + self.__DATABASE_HOSTNAME + " password = '" + self.__DATABASE_PW + "'")
			self.DB_CONNECTION.set_client_encoding('UTF8')

		except psycopg2.Error as e:
			return dict(status = False, error = e)

		return dict(status = True, db_link = self.DB_CONNECTION)

	def db_insert(self, table, data_dict):
		""" 
		This method receives two params.
			- table (string): refers to the table in which we will store the data.
			- data_dict (dict): its a dictionary that has the fields name on dict.keys and values on 
			dict.values.
		The only task this method perform its the insert clause on a table given with their 
		corresponding fields/values.

		returns a dict(status = boolean, sql = string, statusmessage = string)
		"""

		self.sql_fields = ''
		self.sql_dict_names = ''
		self.sql_tuple = ()
		self.sql_sentence = ''

		for i in data_dict:
			self.sql_fields += str(i) + str(',')
			self.sql_dict_names += str('%s,')
			self.sql_tuple += data_dict[i], 

		self.sql_sentence = "INSERT INTO " + str(table) + " (" + self.sql_fields[:-1] + ") VALUES (" + self.sql_dict_names[:-1] + ")"

		self.CURRENT_CONNNECION = self.DB_CONNECTION.cursor()
		try:
			self.CURRENT_CONNNECION.execute(self.sql_sentence, self.sql_tuple)
		except psycopg2.Error as e:
			return dict(status = False, sql = self.CURRENT_CONNNECION.query, error = e, sentence = self.sql_sentence, data = self.sql_tuple)

		self.CURRENT_CONNNECION.close()
		self.DB_CONNECTION.commit()

		return dict(status = True, sql = self.CURRENT_CONNNECION.query, statusmessage = self.CURRENT_CONNNECION.statusmessage)

	def db_select_all(self, table, data_list, filther = None, sort_by = None):
		"""
		This method receives four params.
			- table (string): refers to the table in which we will store the data.
			- data_list (list): its a list that has the fields name to be retrieved.
			- filther (dict): column(s) to be filthered with the WHERE clause.
			- sort_by (list): column(s) used to sort the query result
		Feth the result of a database select, it fetch all data matches with or without filther set.

		return a dict(status = boolean, sql = string, statusmessage = string, data = list of tuples, rowcount = number)
		"""

		self.sql_fields = ''
		self.sql_filther_holder = ''
		self.sql_filther_values = ()
		self.sql_filther_sentence = None
		self.sql_sort_by = ''

		for i in data_list:
			self.sql_fields += str(i) + ','

		if filther:
			for condition in filther:
				self.sql_filther_holder += str(condition) + ' = ' + str('%s and ')
				self.sql_filther_values += filther[condition], 
			self.sql_filther_sentence = ' WHERE ' + self.sql_filther_holder[:-4]

		if sort_by:
			for sortby in sort_by:
				self.sql_sort_by += str(sortby) + ',' 
			self.sql_sort_by = ' ORDER BY ' + self.sql_sort_by[:-1]

		try:
			self.CURRENT_CONNNECION = self.DB_CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
		except psycopg2.Error as e:
			return dict(status = False, error = e)

		self.CURRENT_CONNNECION.execute(
		"""SELECT """ + self.sql_fields[:-1] + """ FROM """ + str(table) + (self.sql_filther_sentence if self.sql_filther_sentence else '') + (self.sql_sort_by if sort_by else ''), 
		((self.sql_filther_values if self.sql_filther_sentence else None) ))

		self.sql_results = self.CURRENT_CONNNECION.fetchall()

		self.CURRENT_CONNNECION.close()
		self.DB_CONNECTION.commit()
		return dict(status = True, sql = self.CURRENT_CONNNECION.query, statusmessage = self.CURRENT_CONNNECION.statusmessage, data = self.sql_results, rowcount = self.CURRENT_CONNNECION.rowcount)

	def db_select_one(self, table, data_list, filther):
		"""
		This method receives three params.
			- table (string): refers to the table in which we will store the data.
			- data_list (list): its a list that has the fields name to be retrieved.
			- filther (tuple): column(s) to be filthered with the WHERE clause.
		Feth the result of a database select, it fetch all data matches with or without filther set.

		return a dict(status = boolean, sql = string, statusmessage = string, data = a tuple, rowcount = number)
		"""

		self.sql_fields = ''

		for i in data_list:
			self.sql_fields += str(i) + ','

		self.sql_filther_holder = ''
		self.sql_filther_values = ()
		self.sql_filther_sentence = None

		if filther:
			for condition in filther:
				self.sql_filther_holder += str(condition) + ' = ' + str('%s and ')
				self.sql_filther_values += filther[condition], 
		self.sql_filther_sentence = ' WHERE ' + self.sql_filther_holder[:-4]

		try:
			self.CURRENT_CONNNECION = self.DB_CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
		except psycopg2.Error as e:
			return dict(status = False, error = e)

		self.CURRENT_CONNNECION.execute(
		"""SELECT """ + self.sql_fields[:-1] + """ FROM """ + str(table) + self.sql_filther_sentence, 
		(self.sql_filther_values ))

		self.sql_result = self.CURRENT_CONNNECION.fetchone()

		self.CURRENT_CONNNECION.close()
		self.DB_CONNECTION.commit()

		return dict(status = True, sql = self.CURRENT_CONNNECION.query, statusmessage = self.CURRENT_CONNNECION.statusmessage, data = self.sql_result, rowcount = self.CURRENT_CONNNECION.rowcount)

	def db_delete(self, table, filther):
		"""
		This method receives two params.
			- table (string): refers to the table in which we will store the data.
			- filther (tuple): column(s) to be filthered with the WHERE delete clause.
		Delete the matched row(s) of a table.

		return a dict(status = boolean, sql = string, statusmessage = string)
		"""

		self.sql_filther_holder = ''
		self.sql_filther_values = ()
		self.sql_filther_sentence = None

		if filther:
			for condition in filther:
				self.sql_filther_holder += str(condition) + ' = ' + str('%s and ')
				self.sql_filther_values += filther[condition], 
		self.sql_filther_sentence = ' WHERE ' + self.sql_filther_holder[:-4]

		try:
			self.CURRENT_CONNNECION = self.DB_CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
		except psycopg2.Error as e:
			return dict(status = False, error = e)

		self.CURRENT_CONNNECION.execute(
		"""DELETE FROM """ + str(table) + self.sql_filther_sentence, 
		(self.sql_filther_values ))

		self.CURRENT_CONNNECION.close()
		self.DB_CONNECTION.commit()

		return dict(status = True, sql = self.CURRENT_CONNNECION.query, statusmessage = self.CURRENT_CONNNECION.statusmessage)

	def db_update(self, table, data_dict, filther):
		"""
		This method receives three params.
			- table (string): refers to the table in which we will store the data.
			- data_dict (dict): its a dictionary that has the fields name on dict.keys and values on 
			dict.values.
			- filther (dict): column(s) to be filthered with the WHERE clause.
		Delete the matched row(s) of a table.

		return a dict(status = boolean, sql = string, statusmessage = string)
		"""

		self.sql_fields_to_update = ''
		self.sql_tuple = ()

		self.sql_filther_holder = ''
		#self.sql_filther_values = ()
		self.sql_filther_sentence = None

		for i in data_dict:
			self.sql_fields_to_update += str(i) + ' = ' + str('%s,')
			self.sql_tuple += data_dict[i], 

		if filther:
			for condition in filther:
				self.sql_filther_holder += str(condition) + ' = ' + str('%s and ')
				self.sql_tuple += filther[condition], 
			self.sql_filther_sentence = ' WHERE ' + self.sql_filther_holder[:-4]

		try:
			self.CURRENT_CONNNECION = self.DB_CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
		except psycopg2.Error as e:
			return dict(status = False, error = e)

		self.CURRENT_CONNNECION.execute(
		"""UPDATE """ + str(table) + ' SET ' + self.sql_fields_to_update[:-1] + (self.sql_filther_sentence if self.sql_filther_sentence else ''), 
		(self.sql_tuple ))

		self.CURRENT_CONNNECION.close()
		self.DB_CONNECTION.commit()
		return dict(status = True, sql = self.CURRENT_CONNNECION.query, statusmessage = self.CURRENT_CONNNECION.statusmessage)


	def db_close(self):
		"""
		This method receives no params.
		Closes the object db connection used across the whole instances of this class.
		"""
		try:
			self.DB_CONNECTION.close()
		except psycopg2.Error as e:
			return dict(status = False, error = e)

		self.DB_CONNECTION = None
		return dict(status = True)