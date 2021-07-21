"""
@Author: Swapnil Bhoyar
@Date: 2021-07-21
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-21
@Title : this program contains view operations.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Views:
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

    def createView(self):
        """
        Describe:
            function to create view
        """
        try:
            sql = "CREATE VIEW student_view AS SELECT s.student_name, s.score, i.exam_no, i.date_of_birth FROM student s, student_info i WHERE s.roll_no=i.roll_no;"
            self.mycursor.execute(sql)
            sql = "select * from student_view;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def deleteView(self):
        """
        Describe:
            function to delete view
        """
        try:
            sql = "DROP VIEW student_view;"
            self.mycursor.execute(sql)
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    view = Views()
    view.createView()
    view.deleteView()