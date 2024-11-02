# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg
import os
from dotenv import load_dotenv, dotenv_values

class PunktozaPipeline:
    def __init__(self):
        # Connect to PostreSQL database
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=dotenv_path)

        hostname = os.getenv("PGHOST")
        username = os.getenv("PGUSER")
        password = os.getenv("PGPASSWORD")
        port = os.getenv("PGPORT")
        database = os.getenv("PGDATABASE")

        self.connection = psycopg.connect(host=hostname, user=username, password=password, dbname=database, port=port)
        self.cur = self.connection.cursor()


    def process_item(self, item, spider):
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()
