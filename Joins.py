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

    def insertData(self):
        """
        Describe:
            function to insert data
        """
        try:
            sql = "INSERT INTO student (roll_no, student_name, score) VALUES (2, 'Peter', 50)"
            self.mycursor.execute(sql)
            self.mydb.commit()
            Log.logging.info("Data insered successfully")
        except Exception as e:
            Log.logging.error(e)

    def printData(self):
        """
        Describe:
            function to print data
        """
        try:
            sql = "SELECT * FROM student"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def createTable(self):
        """
        Describe:
            function to create table
        """
        try:
            sql = "CREATE TABLE student_info (roll_no INT, exam_no INT, date_of_birth DATE)"
            self.mycursor.execute(sql)
        except Exception as e:
            Log.logging.error(e)

    def insertMultipleData(self):
        """
        Describe:
            function to insert multiple data
        """
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

    def createSubjectTable(self):
        """
        Describe:
            function to create subject table
        """
        try:
            sql = "CREATE TABLE subject_info (subject_no INT, subject_name VARCHAR(30))"
            self.mycursor.execute(sql)
        except Exception as e:
            Log.logging.error(e)

    def insertMultipleSubjectData(self):
        """
        Describe:
            function to insert data in subject table
        """
        try:
            sql = "INSERT INTO subject_info (subject_no, subject_name) VALUES (%s, %s)"

            val = [
                (1230, 'Maths'),
                (1240, 'Chemistry'),
                (1250, 'Physics'),
                (1260, 'Biology')
                ]
            self.mycursor.executemany(sql, val)
            self.mydb.commit()
            Log.logging.info("multiple record inserted successfully")
        except Exception as e:
            Log.logging.error(e)

    def innerJoin(self):
        """
        Describe:
            function to inner join
        """
        try:
            sql = "SELECT student_info.exam_no,student_info.date_of_birth,student.student_name FROM student_info INNER JOIN student ON student_info.roll_no=student.roll_no;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def leftJoin(self):
        """
        Describe:
            function to left join
        """
        try:
            sql = "SELECT student_info.exam_no,student_info.date_of_birth,student.student_name FROM student_info LEFT JOIN student ON student_info.roll_no=student.roll_no;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    
    def rightJoin(self):
        """
        Describe:
            function to right join
        """
        try:
            sql = "SELECT student_info.exam_no,student_info.date_of_birth,student.student_name FROM student_info RIGHT JOIN student ON student_info.roll_no=student.roll_no;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def crossJoin(self):
        """
        Describe:
            function to create cross join
        """
        try:
            sql = "SELECT * FROM student_info CROSS JOIN subject_info;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

    def createPrimaryKey(self):
        """
        Description:
            function creates primary key
        """
        self.mycursor.execute("ALTER TABLE student ADD PRIMARY KEY(roll_no);")
        self.mycursor.execute("DESCRIBE student;")
        myresult = self.mycursor.fetchall()
        for x in myresult:
            Log.logging.info(x)
    
    def createForeignKey(self):
        """
        Describe:
            function creates foreign key
        """
        self.mycursor.execute("ALTER TABLE student_info ADD CONSTRAINT FOREIGN KEY(roll_no) REFERENCES student(roll_no);")
        self.mycursor.execute("DESCRIBE student_info;")
        myresult = self.mycursor.fetchall()
        for x in myresult:
            Log.logging.info(x)

    def selfJoin(self):
        """
        Describe:
            function to self join
        """
        try:
            sql = "SELECT s.student_name student_name, m.student_name partner FROM student s INNER JOIN student m ON s.roll_no=m.project_partner;"
            self.mycursor.execute(sql)
            myresult = self.mycursor.fetchall()
            for x in myresult:
                Log.logging.info(x)
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    join = Joins()
    join.createTable()
    join.insertMultipleData()
    join.insertData()
    join.printData()
    join.innerJoin()
    join.leftJoin()
    join.rightJoin()
    join.createSubjectTable()
    join.insertMultipleSubjectData()
    join.printData()
    join.crossJoin()
    join.createPrimaryKey()
    join.createPrimaryKey()
    join.createForeignKey()
    join.selfJoin()