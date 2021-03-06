"""
@Author: Swapnil Bhoyar
@Date: 2021-07-22
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-22
@Title : this program contains window functions.
"""

import mysql.connector
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Window:
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

    def gender_count(self):
        """
        Describe:
            function to get student gender
        """
        try:
            sql = "select student_name, gender, count(gender) over (partition by gender) as gender_count from student;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def avg_score(self):
        """
        Describe:
            function to get student avg score
        """
        try:
            sql = "select student_name, gender, avg(score) over (partition by gender) as totalgender from student;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def rank(self):
        """
        Describe:
            function to get student rank
        """
        try:
            sql = "select student_name, rank() over (order by score desc) as rank_number from student;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def lag(self):
        """
        Describe:
            function to get student rank
        """
        try:
            sql = "select student_name, lag(score,1) over (order by score desc) as lag_score  from student;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def n_tile(self):
        """
        Describe:
            function to get student in tiles
        """
        try:
            sql = "select student_name, ntile(4) over (order by score desc) as 'row_number'  from student;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    window = Window()
    window.gender_count()
    window.avg_score()
    window.rank()
    window.lag()
    window.n_tile()

