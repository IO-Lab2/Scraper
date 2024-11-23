# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import psycopg2
from attrs import field
from dotenv import load_dotenv
from itemadapter import ItemAdapter
from sggwScraper.items import ScientistItem, publicationItem, organizationItem


class SggwscraperPipeline:
    def process_item(self, item, spider):

        adapter=ItemAdapter(item)

        def try_parse_int(value):
            try:
                int(value)
                return True
            except ValueError:
                return False

        def clean_str_int(field_names, adapter):
            for field_name in field_names:
                value=adapter.get(field_name)
                if value and isinstance(value, str):
                    adapter[field_name]=value.strip()
                if isinstance(value, str) and try_parse_int(value) and field_name!='publication_date':
                    adapter[field_name]=int(value)
                if isinstance(value, list):
                    for v in value:
                        if isinstance(v, str) and try_parse_int(v):
                            adapter[field_name]=[v.strip() if isinstance(v, str) else v for v in value]
                            

        

        if isinstance(item, ScientistItem):
            field_names=adapter.field_names()

            academic_title=adapter.get('academic_title')
            if academic_title:
                adapter['academic_title']=academic_title.strip(', ')
            ministerial_score=adapter.get('ministerial_score')
            if isinstance(ministerial_score, str):
                adapter['ministerial_score']=ministerial_score.replace(',','')

            research_area=adapter.get('research_area')
            if research_area:
                adapter['research_area']=', '.join(research_area)

            clean_str_int(field_names, adapter)


        elif isinstance(item, publicationItem):
            field_names=adapter.field_names()
            authors=adapter.get('authors')

            for i in range(len(authors)):
                for k in range(len(authors[i])):
                    if isinstance(authors[i][k], str):
                        adapter['authors'][i][k]=authors[i][k].strip()

            clean_str_int(field_names, adapter)
            pub_date=adapter.get('publication_date')
            if pub_date:
                adapter['publication_date']=pub_date+'-01-01'

        elif isinstance(item, organizationItem):
            field_names=adapter.field_names()
            clean_str_int(field_names, adapter)
            
        return item
    

class SaveToDataBase:
    def open_spider(self, spider):
        load_dotenv()
        self.conn = psycopg2.connect(
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            host=os.getenv("PGHOST"),
            port=os.getenv("PGPORT")
        )
        self.cursor = self.conn.cursor()
        
        #DELETE FROM organizations;
        '''
        self.cursor.execute("""
            DELETE FROM bibliometrics;
            DELETE FROM scientists;
            
            
            DELETE FROM publications;
            
            INSERT INTO
            organizations (
                name,
                type
                )
                VALUES (
                    'SGGW',
                    'university'
                    );
            """)
        '''

    def close_spider(self, spider):
        # Commit any remaining data and close the connection when the spider finishes
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


    def process_item(self, item, spider):
        
        adapter=ItemAdapter(item)
        
        if isinstance(item, ScientistItem):
            self.cursor.execute("""
                
                INSERT INTO
                scientists (
                    first_name,
                    last_name,
                    academic_title,
                    research_area,
                    email,
                    profile_url
                )
                VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                        ) RETURNING id;
                """,
                (
                    adapter['first_name'],
                    adapter['last_name'],
                    adapter['academic_title'],
                    adapter['research_area'],
                    adapter['email'],
                    adapter['profile_url']
                ))
            
            scientis_id=self.cursor.fetchone()[0]

            self.cursor.execute("""
                
                INSERT INTO
                bibliometrics (
                    h_index_wos,
                    h_index_scopus,
                    publication_count,
                    ministerial_score,
                    scientist_id
                )
                VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                        );
                """,
                (
                    adapter['h_index_wos'],
                    adapter['h_index_scopus'],
                    adapter['publication_count'],
                    adapter['ministerial_score'],
                    scientis_id
                ))
            

            self.cursor.execute(f"SELECT id FROM organizations WHERE name like '{adapter['organization']}'")

            org_id=self.cursor.fetchone()[0]

            if org_id:
                self.cursor.execute("""
                
                INSERT INTO
                scientist_organization (
                    scientist_id,
                    organization_id
                )
                VALUES (
                        %s,
                        %s
                        );
                """,
                (
                    scientis_id,
                    org_id
                ))
        elif isinstance(item, publicationItem):
            self.cursor.execute("""
                
                INSERT INTO
                publications (
                    title,
                    journal,
                    publication_date,
                    citations_count,
                    journal_impact_factor
                )
                VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                        );
                """,
                (
                    adapter['title'],
                    adapter['journal'],
                    adapter['publication_date'],
                    adapter['citations_count'],
                    0,
                ))
            
        elif isinstance(item, organizationItem):
            org_type=['institute', 'cathedra']
            
            if adapter['institute']:
                self.cursor.execute("""
                    
                    INSERT INTO
                    organizations (
                        name,
                        type
                    )
                    VALUES (
                            %s,
                            %s
                            );
                    """,
                    (
                        adapter['institute'],
                        org_type[0],
                    ))
            if adapter['cathedras']:
                for cathedra in adapter['cathedras']:
                    self.cursor.execute("""
                
                    INSERT INTO
                    organizations (
                        name,
                        type
                    )
                    VALUES (
                            %s,
                            %s
                            );
                    """,
                    (
                        cathedra,
                        org_type[1]
                    ))
            

        return item
    