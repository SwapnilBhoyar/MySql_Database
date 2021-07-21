"""
@Author: Swapnil Bhoyar
@Date: 2021-07-21
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-21
@Title : this program contains indexes operations.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Indexes:
    def __init__(self):
        """
        Describe:
            function to create connection
        """
        try:
            self.mydb = mysql.connector.connect(
            host = env['DB_HOST'],
            user=env['DB_USER'],
            password=env['DB_PASSWORD'],
            database =env['DB_DATABASE'])
            self.mycursor = self.mydb.cursor()  
        except Exception as e:
            Log.logging.error(e)

    def createIndex(self):
        """
        Describe:
            function to create index
        """
        try:
            sql = "CREATE INDEX student_index ON student_info(exam_no);"
            self.mycursor.execute(sql)
            Log.logging.info("Index created")
        except Exception as e:
            Log.logging.error(e)

    def deleteIndex(self):
        """
        Describe:
            function to delete index
        """
        try:
            sql = "ALTER TABLE student_info DROP INDEX student_index;"
            self.mycursor.execute(sql)
            Log.logging.info("Index deleted")
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    index = Indexes()

    index.createIndex()
    index.deleteIndex()