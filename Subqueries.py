"""
@Author: Swapnil Bhoyar
@Date: 2021-07-22
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-22
@Title : this program contains subqueries operations.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Subqueries:
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

    def comparision(self):
        """
        Describe:
            function to get student with min marks
        """
        try:
            sql = "SELECT * FROM student WHERE score = (SELECT MIN(score) FROM student);"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    subqueries = Subqueries()
    subqueries.comparision()

