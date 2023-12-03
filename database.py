import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

cnx = mysql.connector.connect(
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PWD'),
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_DATABASE_NAME')
)
cursor = cnx.cursor()

def insert_raw(topic, message):
    query = "insert into houses_raw (topic, message) values (%s, %s)"
    
    cursor.execute(query, (topic, message))
    cnx.commit()
    print('saved: ' +topic +' '+message)
