# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from attrs import field
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

        def clean_str_int(filed_names, adapter):
            for field_name in field_names:
                value=adapter.get(field_name)
                if value and isinstance(value, str):
                    adapter[field_name]=value.strip()
                if isinstance(value, str) and try_parse_int(value):
                    adapter[field_name]=int(value)

        

        if isinstance(item, ScientistItem):
            field_names=adapter.field_names()
            clean_str_int(field_names, adapter)

            value=adapter.get('academic_title')
            adapter['academic_title']=value.strip(', ')

        elif isinstance(item, publicationItem):
            field_names=adapter.field_names()
            clean_str_int(field_names, adapter)

        elif isinstance(item, organizationItem):
            field_names=adapter.field_names()
            clean_str_int(field_names, adapter)


        
            
        return item
