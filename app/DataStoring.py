import sys
import os
module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.append(module_path)

# 1. Crawling Data


from Module.Crawling import Dataset
NewsData = Dataset()
page = 10
per_page = 10
title, link, desc, pubdate = NewsData.DataList("금리", page, per_page)

# 2. Connect Postgres

from Module.config import get_secret
host = get_secret("host")
dbname = get_secret("dbname")
user = get_secret("user")
port = get_secret("port")
password = get_secret("postgresDB_password")


import  psycopg2
postgresDB = psycopg2.connect(
    host = host, dbname = dbname,
    user = user, password = password, port = port
    )

cursor = postgresDB.cursor()

# 3. Storing data to DB
for idx in range(page*per_page):
    cursor.execute(
        "insert into airflow_pjt.news_data (title, link, description, pubdate) values (%s, %s, %s, %s);",
        (title[idx], link[idx], desc[idx], pubdate[idx])
    )
    
postgresDB.commit()

# 4. connect shutdown
cursor.close()
postgresDB.close()
