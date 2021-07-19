"""
@Author: Swapnil Bhoyar
@Date: 2021-07-20
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-20
@Title : this program contains crud operations.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/data.env')

class Operations:
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

    def insertData(self):
        try:
            sql = "INSERT INTO student (roll_no, student_name, score) VALUES (1, 'swapnil', 75)"
            self.mycursor.execute(sql)
            self.mydb.commit()
            Log.logging.info("Data insered successfully")
        except Exception as e:
            Log.logging.error(e)

    def printData(self):
        try:
            self.mycursor.execute("SELECT * FROM student")
            myresult = self.mycursor.fetchone()
            Log.logging.info(myresult)
        except Exception as e:
            Log.logging.error(e)

    def updateData(self):
        try:
            sql = "UPDATE student SET student_name = 'aditya' WHERE roll_no = 1"
            self.mycursor.execute(sql)
            self.mydb.commit
            Log.logging.info(self.mycursor.rowcount, "record(s) affected")
            Log.logging.info("Data updated successfully")
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    operation = Operations()
    operation.insertData()
    operation.printData()
    operation.updateData()
    operation.mydb.close()