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

load_dotenv('/home/neo/Programs/MySql_Database/.env')

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
            sql = "INSERT INTO student (roll_no, student_name, score) VALUES (1, 'swapnil', 76)"
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
            Log.logging.info("Data updated successfully")
        except Exception as e:
            Log.logging.error(e)

    def printData(self):
        try:
            self.mycursor.execute("SELECT * FROM student")
            myresult = self.mycursor.fetchall()
            # for x in myresult:
            #     Log.logging.info(x)
            Log.logging.info(myresult)
        except Exception as e:
            Log.logging.error(e)

    def deleteData(self):
        try:
            sql = "DELETE FROM student WHERE roll_no = 2"
            self.mycursor.execute(sql)
            self.mydb.commit()
            Log.logging.info("Data deleted successfully")
        except Exception as e:
            Log.logging.error(e)

    def insertMultipleData(self):
        try:
            sql = "INSERT INTO student (roll_no, student_name, score) VALUES (%s, %s, %s)"

            val = [
                (2,'Peter', 50),
                (3,'Amy', 60),
                (4,'Hannah', 76),
                (5,'Michael', 87)
                ]
            self.mycursor.executemany(sql, val)
            self.mydb.commit()
            Log.logging.info("multiple record inserted successfully")
        except Exception as e:
            Log.logging.error(e)

    def sortData(self):
        try:
            sql = "SELECT * FROM student ORDER BY student_name"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def groupBy(self):
        try:
            sql = "SELECT score FROM student GROUP BY score"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def limitData(self):
        try:
            sql = "SELECT * FROM student LIMIT 2"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    
    def distinctData(self):
        try:
            sql = "SELECT DISTINCT student_name FROM student"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)


if __name__=="__main__":
    operation = Operations()

    # operation.insertData()
    # operation.printData()
    # operation.updateData()
    # operation.printData()
    # operation.deleteData()
    # operation.insertMultipleData()
    # operation.printData()
    # operation.sortData()
    # operation.limitData()
    # operation.groupBy()
    # operation.printData()
    operation.distinctData()

    operation.mydb.close()