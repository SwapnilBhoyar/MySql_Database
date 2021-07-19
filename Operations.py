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
            Log.logging.info(e)
 
if __name__=="__main__":
    operation = Operations()
    operation.mydb.close()