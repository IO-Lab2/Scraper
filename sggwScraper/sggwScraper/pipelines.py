# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import os
import psycopg2
from dotenv import load_dotenv
from itemadapter import ItemAdapter
from sggwScraper.items import ScientistItem, publicationItem, organizationItem


class SggwscraperPipeline:
    university='Warsaw University of Life Sciences - SGGW'
    @staticmethod
    def get_author_id_from_url(url):
                result=url.split('WULS')[1].split('?')
                return "WULS"+result[0] 
    
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
                    clear_list=[]
                    for v in value:
                        if isinstance(v, str) and try_parse_int(v):
                            adapter[field_name]=[v.strip() if isinstance(v, str) else v for v in value]
                        if isinstance(v, str):
                            clear_list.append(v.strip())
                            adapter[field_name]=clear_list
                                              

        

        if isinstance(item, ScientistItem):
            field_names=adapter.field_names()

            academic_title=adapter.get('academic_title')
            if academic_title:
                adapter['academic_title']=academic_title.strip(', ')
            ministerial_score=adapter.get('ministerial_score')
            if isinstance(ministerial_score, str):
                adapter['ministerial_score']=ministerial_score.replace(',','')

            research_area=adapter.get('research_area')
            if research_area and research_area=='nutrition and food technology (FNT)':
                adapter['research_area']='food and nutrition technology (FNT)'


            clean_str_int(field_names, adapter)

            organization=adapter.get('organization')
            if organization:
                org_dict={}
                if len(organization)==2:
                    org_dict['university']=self.university
                    org_dict['institute']=organization[1]
                    org_dict['cathedra']=organization[0]
                elif len(organization)==1:
                    org_dict['university']=self.university
                    org_dict['institute']=organization[0]
                adapter['organization']=org_dict

        elif isinstance(item, publicationItem):
            field_names=adapter.field_names()
            
            authors=adapter.get('authors')
            adapter['authors']=[self.get_author_id_from_url(link) for link in authors]
            

            clean_str_int(field_names, adapter)

            pub_date=adapter.get('publication_date')
            if pub_date:
                adapter['publication_date']=pub_date+'-01-01'

            adapter['title']+=', vol: '+adapter.get('vol') if adapter.get('vol') else ''

            if adapter['journal']:
                adapter['journal']=adapter['journal'].split(', ISSN')[0]

            

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

        print(f'SPIDER: {spider.name} STARTED')
        
        

    def close_spider(self, spider):
        # Commit any remaining data and close the connection when the spider finishes
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


    def process_item(self, item, spider):
        
        adapter=ItemAdapter(item)
        
        if isinstance(item, ScientistItem):
            
            
            scientistFields=tuple(ItemAdapter(item).values())

            def add_scientist():
                self.cursor.execute(""" 
                    INSERT INTO
                    scientists (
                        first_name,
                        last_name,
                        academic_title,
                        email,
                        profile_url,
                        position
                    )
                    VALUES (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                            ) RETURNING id;
                    """,scientistFields[:6])  
                
                return self.cursor.fetchone()[0]
                                     
            def update_or_create_scientist():   
                wuls=SggwscraperPipeline.get_author_id_from_url(adapter['profile_url'])
                self.cursor.execute("""
                    SELECT
                        id,
                        first_name,
                        last_name,
                        academic_title,
                        email,
                        profile_url,
                        position
                    FROM scientists
                    WHERE profile_url like %s;
                    """, ('%'+wuls+'%',)      
                )

                scientist_in_db=self.cursor.fetchone()
                if scientist_in_db:
                    if scientist_in_db[1:]!=scientistFields[:6]:
                        #update scientist
                        self.cursor.execute("""
                            UPDATE scientists
                            SET
                                first_name = %s,
                                last_name = %s,
                                academic_title = %s,
                                email = %s,
                                profile_url = %s,
                                updated_at = CURRENT_TIMESTAMP,
                                position=%s
                            WHERE id = %s;
                            """, 
                        scientistFields[:6]+(scientist_in_db[0],))

                    return scientist_in_db[0]
                else:
                    return add_scientist()

            def add_bibliometrics(scientist_id):
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
                        scientistFields[6:10]+(scientist_id,))

            def update_or_create_bibliometrics(scientist_id):
                self.cursor.execute("""
                    SELECT
                        h_index_wos,
                        h_index_scopus,
                        publication_count,
                        ministerial_score
                    FROM bibliometrics
                    WHERE scientist_id = %s;
                    """, (scientist_id,))

                bibliometrics_in_db=self.cursor.fetchone()
                if bibliometrics_in_db:
                    if bibliometrics_in_db!=scientistFields[6:10]:
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
                        scientistFields[6:10]+(scientist_id,))
                else:
                    add_bibliometrics(scientist_id)
            
            def update_or_create_organization_relationship(scientist_id):
                
                for key in adapter['organization']:
                    
                    self.cursor.execute(f"SELECT id FROM organizations WHERE name like '{adapter['organization'][key]}';")
                    org_id=self.cursor.fetchone()[0]

                    self.cursor.execute("""
                                SELECT organization_id, so.id 
                                FROM scientist_organization so 
                                INNER JOIN organizations o ON so.organization_id = o.id
                                WHERE scientist_id = %s AND type=%s;""", (scientist_id, key))
                    
                    scientist_in_org=self.cursor.fetchone()
                    
                    
                    if scientist_in_org:
                        if scientist_in_org[0]!=org_id:
                            #update organization
                            self.cursor.execute("""
                                UPDATE scientist_organization
                                SET
                                    organization_id = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s;
                                """,
                                (
                                    org_id,
                                    scientist_in_org[1]
                                ))
                    else:
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
   
            def add_research_area(ra_name):
                self.cursor.execute(""" 
                    INSERT INTO
                    research_areas(
                        name
                    )
                    VALUES (
                            %s
                            ) RETURNING id;
                    """,
                    (
                        ra_name,
                    ))
                return self.cursor.fetchone()[0]
            
            def add_reserach_area_relationship(scientist_id, ra_id):
                self.cursor.execute(""" 
                        INSERT INTO
                        scientists_research_areas (
                            scientist_id,
                            research_area_id
                            )
                        VALUES (
                                %s,
                                %s
                                );
                        """,
                            (
                                scientist_id,
                                ra_id
                            ))
 
            def get_or_create_research_area_relationship(scientist_id):
                scraped_ra_ids=[]
                for research_area in adapter['research_area']:
                    self.cursor.execute(f"SELECT id FROM research_areas WHERE name like '{research_area}';")
                    ra_id_in_db=self.cursor.fetchone()
                    if ra_id_in_db:
                        scraped_ra_ids.append(ra_id_in_db[0])
                    else:
                        scraped_ra_ids.append(add_research_area(research_area))
                
                self.cursor.execute("SELECT research_area_id FROM scientists_research_areas WHERE scientist_id = %s;", (scientist_id,))
                selected_ra_ids=[row[0] for row in self.cursor.fetchall()]

                for ra_id in scraped_ra_ids:
                    if ra_id not in selected_ra_ids:
                        add_reserach_area_relationship(scientist_id, ra_id)
                        
                            

            #scientist table
            scientist_id=update_or_create_scientist()

            #bibliometrics table
            update_or_create_bibliometrics(scientist_id)

            #scientist_organization table
            update_or_create_organization_relationship(scientist_id)

            #scientist_research_areas table
            get_or_create_research_area_relationship(scientist_id)
            
            

        elif isinstance(item, publicationItem):

            def add_publication(title, publisher, publication_date, journal, ministerial_score):
                self.cursor.execute("""
                    INSERT INTO 
                    publications 
                    (title, publisher, publication_date, journal_impact_factor, journal, ministerial_score) 
                    VALUES (%s, %s, %s, 0, %s, %s) RETURNING id;""", 
                    (title, publisher, publication_date, journal, ministerial_score))
                return self.cursor.fetchone()[0]

            def get_or_create_publication(title, publisher, publication_date, journal, ministerial_score):
                self.cursor.execute("""
                        SELECT id, journal, ministerial_score
                        FROM publications 
                        WHERE title = %s  
                        AND (publication_date = %s OR publication_date IS NULL)
                        ;""", (title, publication_date))
                
                result=self.cursor.fetchone()
                if result:
                    #update publication
                    if result[1:3]!=(journal, ministerial_score,):
                        self.cursor.execute("""
                            UPDATE publications
                            SET
                                journal = %s,
                                ministerial_score = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s;
                            """, (journal, ministerial_score, result[0]))

                    return result[0]
                else:
                    return add_publication(title, publisher, publication_date, journal, ministerial_score)

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
                        publication_id=get_or_create_publication(adapter['title'], adapter['publisher'], adapter['publication_date'], adapter['journal'], adapter['ministerial_score'])
                        get_or_create_author_publications(scientist_id, publication_id)
                
            
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
            
        self.conn.commit()
        return item
    