"""
@Author: Swapnil Bhoyar
@Date: 2021-07-22
@Last Modified by: Swapnil Bhoyar
@Last Modified time: 2021-07-22
@Title : this program contains import export functions.
"""

import mysql.connector
import os
from os import environ as env   
from dotenv import load_dotenv
import Log

load_dotenv('/home/neo/Programs/MySql_Database/.env')

class Import_export:
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

    def import_database(self):
        """
        Describe:
            function to get student gender
        """
        try:
            filename = 'data-dump.sql'
            # os.system('mysqldump -u root -p student_record > data-dump.sql')
            os.system('mysql -u{} -p{} {} < {}'.format(self.mydb.user, self.mydb._password, self.mydb.database, filename))
            Log.logging.info("import successful")
        except Exception as e:
            Log.logging.error(e)

if __name__=="__main__":
    import_export = Import_export()
    import_export.import_database()