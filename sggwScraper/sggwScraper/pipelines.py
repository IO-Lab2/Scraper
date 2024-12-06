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
                if isinstance(value, str) and try_parse_int(value) and field_name not in ['publication_date', 'vol']:
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
            
            
            def get_author_id_from_url(url):
                result=url.split('WULS')[1].split('?')
                return "WULS"+result[0]
            
            authors=adapter.get('authors')
            adapter['authors']=[get_author_id_from_url(link) for link in authors]
            

            clean_str_int(field_names, adapter)

            pub_date=adapter.get('publication_date')
            if pub_date:
                adapter['publication_date']=pub_date+'-01-01'

            adapter['title']+=', vol: '+adapter.get('vol') if adapter.get('vol') else ''

            

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
        
        

    def close_spider(self, spider):
        # Commit any remaining data and close the connection when the spider finishes
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


    def process_item(self, item, spider):
        
        adapter=ItemAdapter(item)
        
        if False:#isinstance(item, ScientistItem):
            #scientist table
            self.cursor.execute("""
                SELECT
                    s.id,
                    first_name,
                    last_name,
                    academic_title,
                    research_area,
                    email,
                    profile_url,
                    college,
                    h_index_wos,
                    h_index_scopus,
                    publication_count,
                    ministerial_score,
                    name
                FROM 
                    scientists s
                INNER JOIN 
                    bibliometrics b ON s.id = b.scientist_id
                INNER JOIN
                    scientist_organization so ON s.id = so.scientist_id
                INNER JOIN
                    organizations o ON so.organization_id = o.id
                WHERE profile_url like %s;
                """, (
                adapter['profile_url'],
            ))
            scientist_in_db = self.cursor.fetchone()
            scientistFields=tuple(ItemAdapter(item).values())
            

            if scientist_in_db:
                scientist_id=scientist_in_db[0]
                if scientist_in_db[1:8]!=scientistFields[:7]:
                    #update scientist
                    self.cursor.execute("""
                        UPDATE scientists
                        SET
                            first_name = %s,
                            last_name = %s,
                            academic_title = %s,
                            research_area = %s,
                            email = %s,
                            profile_url = %s,
                            updated_at = CURRENT_TIMESTAMP,
                            college=%s
                        WHERE id = %s;
                        """, 
                    (
                        adapter['first_name'],
                        adapter['last_name'],
                        adapter['academic_title'],
                        adapter['research_area'],
                        adapter['email'],
                        adapter['profile_url'],
                        adapter['college'],
                        scientist_id
                    ))
                elif scientist_in_db[8:-1]!=scientistFields[7:-1]:
                    #update bibliometrics
                    self.cursor.execute("""
                        UPDATE bibliometrics
                        SET
                            h_index_wos = %s,
                            h_index_scopus = %s,
                            publication_count = %s,
                            ministerial_score = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE scientist_id = %s;
                        """,
                    (  
                        adapter['h_index_wos'],
                        adapter['h_index_scopus'],
                        adapter['publication_count'],
                        adapter['ministerial_score'],
                        scientist_id
                    ))
                elif scientist_in_db[-1]!=scientistFields[-1]:
                    #update organization
                    self.cursor.execute(f"SELECT id FROM organizations WHERE name like '{adapter['organization']}'")
                    org_id=self.cursor.fetchone()[0]

                    self.cursor.execute("""
                        UPDATE scientist_organization
                        SET
                            organization_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE scientist_id = %s;
                        """,
                    (
                        org_id,
                        scientist_id
                    ))
                
            else:
                self.cursor.execute("""
                    
                    INSERT INTO
                    scientists (
                        first_name,
                        last_name,
                        academic_title,
                        research_area,
                        email,
                        profile_url,
                        college
                    )
                    VALUES (
                            %s,
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
                        adapter['profile_url'],
                        adapter['college']
                    ))
                
                scientist_id=self.cursor.fetchone()[0]

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
                        scientist_id
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
                        scientist_id,
                        org_id
                    ))


        elif isinstance(item, publicationItem):

            def add_publication(title, publisher, publication_date):
                self.cursor.execute("""
                    INSERT INTO 
                    publications 
                    (title, publisher, publication_date, journal_impact_factor) 
                    VALUES (%s, %s, %s, %s) RETURNING id;""", 
                    (title, publisher, publication_date, 0))
                return self.cursor.fetchone()[0]

            def get_or_create_publication(tittle, publisher, publication_date):
                if publisher is None:
                    self.cursor.execute("""SELECT id FROM publications WHERE title like %s AND publisher is NULL AND publication_date = %s;""", (tittle, publication_date))
                elif publication_date is None:
                    self.cursor.execute("""SELECT id FROM publications WHERE title like %s AND publisher like %s AND publication_date is NULL;""", (tittle, publisher))
                elif publisher is None and publication_date is None:
                    self.cursor.execute("""SELECT id FROM publications WHERE title like %s AND publisher is NULL AND publication_date is NULL;""", (tittle,))
                else:
                    self.cursor.execute("""SELECT id FROM publications WHERE title like %s AND publisher like %s AND publication_date = %s;""", (tittle, publisher, publication_date))
                result=self.cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return add_publication(tittle, publisher, publication_date)

            def add_author_to_publications(scientist_id, publication_id):
                self.cursor.execute("""
                    INSERT INTO 
                    scientists_publications 
                    (scientist_id, publication_id) 
                    VALUES (%s, %s) RETURNING id;""", 
                    (scientist_id, publication_id))
                return self.cursor.fetchone()[0]

            def get_or_create_author_publications(scientist_id, publication_id):
                self.cursor.execute("""
                    SELECT scientist_id 
                    FROM scientists_publications
                    WHERE scientist_id=%s AND publication_id=%s;""", 
                    (scientist_id, publication_id))
                result=self.cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return add_author_to_publications(scientist_id, publication_id)
                
            if adapter['authors']:
                for author_id in adapter['authors']:
                    self.cursor.execute("SELECT id FROM scientists WHERE profile_url like %s;", ('%'+author_id+'%',))
                    result=self.cursor.fetchone()
                    if result:
                        scientist_id=result[0]
                        publication_id=get_or_create_publication(adapter['title'], adapter['publisher'], adapter['publication_date'])
                        get_or_create_author_publications(scientist_id, publication_id)
                
            '''
            self.cursor.execute("""
                SELECT
                    p.id,
                    title,
                    journal,
                    publication_date
                FROM publications p 
                INNER JOIN scientists_publications sp ON p.id = sp.publication_id
                WHERE title like %s AND journal like %s AND publication_date like %s;                
            """,
            (
                adapter['title'],
                adapter['journal'],
                adapter['publication_date']
            ))
                
            publication_in_db=self.cursor.fetchone()

            if publication_in_db:
                self.cursor.execute("""
                    SELECT 
                        first_name, 
                        last_name 
                    FROM scientists_publications sp 
                    INNER JOIN scientists s ON sp.scientist_id=s.id 
                    WHERE publication_id = %s;
                    """,( 
                    publication_in_db[0],))

            else:
                self.cursor.execute("""
                    
                    INSERT INTO
                    publications (
                        title,
                        journal,
                        publication_date,
                        journal_impact_factor
                    )
                    VALUES (
                            %s,
                            %s,
                            %s,
                            %s
                            ) RETURNING id;
                    """,
                    (
                        adapter['title'],
                        adapter['journal'],
                        adapter['publication_date'],
                        0
                    ))
                publication_id=self.cursor.fetchone()[0]

                for full_name in adapter['authors']:
                    self.cursor.execute("SELECT id FROM scientists WHERE first_name = %s AND last_name = %s;", (full_name[0], full_name[1]))
                    author_id=self.cursor.fetchone()
                    if author_id:
                        self.cursor.execute(
                            """
                            INSERT INTO
                            scientists_publications (
                                scientist_id,
                                publication_id
                            )
                            VALUES (
                                    %s,
                                    %s
                                    );
                            """,
                            (
                                author_id[0],
                                publication_id
                            ))
                                
            '''
        elif isinstance(item, organizationItem):
            
            def add_organization(name, organization_type):
                self.cursor.execute("""
                    
                    INSERT INTO
                    organizations (
                        name,
                        type
                    )
                    VALUES (
                            %s,
                            %s
                            ) RETURNING id;
                    """,
                    (
                        name,
                        organization_type,
                    ))
                return self.cursor.fetchone()[0]
            
            def get_or_create_organization(name, organization_type):
                self.cursor.execute("SELECT id FROM organizations WHERE name like %s AND type=%s;", (name, organization_type))
                result=self.cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return add_organization(name, organization_type)

            def add_relationship(parent_id, child_id):
                self.cursor.execute("""
                    
                    INSERT INTO
                    organizations_relationships (
                        parent_id,
                        child_id
                    )
                    VALUES (
                            %s,
                            %s
                            );
                    """,
                    (
                        parent_id,
                        child_id
                    ))
                
            def get_or_create_relationship(parent_id, child_id):
                if parent_id is None:
                    self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id IS NULL AND child_id=%s;", (child_id,))
                elif child_id is None:
                    self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id IS NULL;", (parent_id,))
                else:
                    self.cursor.execute("SELECT id FROM organizations_relationships WHERE parent_id=%s AND child_id=%s;", (parent_id, child_id))
                if self.cursor.fetchone() is None:
                    add_relationship(parent_id, child_id)

            

            university_id = get_or_create_organization(adapter['university'], 'university')
            get_or_create_relationship(None, university_id)

            institute_id = get_or_create_organization(adapter['institute'], 'institute')
            get_or_create_relationship(university_id, institute_id)

            if adapter['cathedras']:
                for cathedra in adapter['cathedras']:
                    cathedra_id = get_or_create_organization(cathedra, 'cathedra')
                    get_or_create_relationship(institute_id, cathedra_id)
                    get_or_create_relationship(cathedra_id, None)
            else:
                get_or_create_relationship(institute_id, None)
            

        return item
    