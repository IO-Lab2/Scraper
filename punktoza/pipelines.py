# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg
import os
from dotenv import load_dotenv, dotenv_values
from scrapy.exceptions import DropItem

class PunktozaPipeline:
    def __init__(self):
        # Connect to PostreSQL database
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=dotenv_path)

        self.hostname = os.getenv("PGHOST")
        self.scraper_username = os.getenv("PGUSERSCRAPER")
        self.scraper_password = os.getenv("PGPASSWORDSCRAPER")
        self.port = os.getenv("PGPORT")
        self.database = os.getenv("PGDATABASE")

        self.connection = psycopg.connect(host=self.hostname, user=self.scraper_username, 
                                          password=self.scraper_password, dbname=self.database, port=self.port)
        self.cur = self.connection.cursor()
    def process_item(self, item, spider):
        # Update impact factor points for every journal in the database
        adapter = ItemAdapter(item)

        journal = adapter.get("name")
        impact_factor = adapter.get("if_points")
        publisher = adapter.get("publisher")
        journal_type = adapter.get("journal_type")

        if journal and impact_factor:
            if publisher:
                update_query = """
                UPDATE publications
                SET journal_impact_factor = %s,
                    publisher = %s,
                    journal_type = %s
                WHERE journal = %s;
            """
            else:
                update_query = """
                UPDATE publications
                SET journal_impact_factor = %s,
                    journal_type = %s
                WHERE journal = %s;
            """
            try:
                self.cur.execute(update_query, (impact_factor, publisher, journal_type, journal))
                self.connection.commit()    
                spider.logger.info(f"Updated impact_factor for journal: {journal}")

            except Exception as e:
                spider.logger.error(f"Error while updating impact_factor for journal {journal}: {e}")
                raise DropItem(f"Failed to update impact_factor for journal {journal}")
            
        else:
            missing_field = "journal" if not journal else "impact_factor"
            spider.logger.warning(f"Item missing required field: {missing_field}")
            raise DropItem(f"Missing {missing_field} for item: {adapter.asdict()}")
        
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

class NameFilterPipeline:
    # Filter out journals that are not in the database
    def __init__(self):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        load_dotenv(dotenv_path=dotenv_path)

        self.hostname = os.getenv("PGHOST")
        self.reader_username = os.getenv("PGUSERREADER")
        self.reader_password = os.getenv("PGPASSWORDREADER")
        self.port = os.getenv("PGPORT")
        self.database = os.getenv("PGDATABASE")

        self.connection = psycopg.connect(host=self.hostname, user=self.reader_username, 
                                          password=self.reader_password, dbname=self.database, port=self.port)
        self.cur = self.connection.cursor()

        # Test with names given in txt file
        # journal_names_path = os.path.join(os.path.dirname(__file__), '..', 'journal_names.txt')
        # with open(journal_names_path, 'r') as file:
        #     lines = file.readlines()

        # self.journal_array = [line.strip() for line in lines]

    def process_item(self, item, spider):
        # Test for txt file
        # if item["name"] in self.journal_array:
        #     return item
        # else:
        #     raise DropItem("Item not in filtered journal names")

        # Check for item's journal name in database
        adapter = ItemAdapter(item)

        journal = adapter.get("name")

        if journal:
            check_query = """
                SELECT 1 FROM publications WHERE journal = %s;
            """
            try:
                self.cur.execute(check_query,(journal,))
                result = self.cur.fetchone()

                if result:
                    return item
                else:
                    raise DropItem(f"Journal {journal} not found in the database")
                
            except Exception as e:
                spider.logger.error(f"Error while processing journal {journal}: {e}")
                raise DropItem(f"Error processing item with journal {journal}")
            
        spider.logger.warning("Item missing journal name")
        raise DropItem("Item missing journal name")
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()