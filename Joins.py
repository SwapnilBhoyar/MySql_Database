"""
@Author: Swapnil Bhoyar
@Date: 2021-07-20
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-20
@Title : this program contains join operations.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Joins:
    def __init__(self):
        try:
            self.mydb = mysql.connector.connect(
            host = env['DB_HOST'],
            user=env['DB_USER'],
            password=env['DB_PASSWORD'],
            database =env['DB_DATABASE'])
            self.mycursor = self.mydb.cursor()  
        except Exception as e:
            Log.logging.error(e)

    def printData(self):
        try:
            sql = "SELECT * FROM student_info"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def createTable(self):
        try:
            sql = "CREATE TABLE student_info (roll_no INT, exam_no INT, date_of_birth DATE)"
            self.mycursor.execute(sql)
        except Exception as e:
            Log.logging.error(e)

    def insertMultipleData(self):
        try:
            sql = "INSERT INTO student_info (roll_no, exam_no, date_of_birth) VALUES (%s, %s, %s)"

            val = [
                (2, 123, '1997-01-01'),
                (3, 124, '1997-02-02'),
                (4, 125, '1997-03-03'),
                (5, 126, '1997-04-04')
                ]
            self.mycursor.executemany(sql, val)
            self.mydb.commit()
            Log.logging.info("multiple record inserted successfully")
        except Exception as e:
            Log.logging.error(e)


if __name__=="__main__":
    join = Joins()
    join.createTable()
    join.insertMultipleData()
    join.printData()