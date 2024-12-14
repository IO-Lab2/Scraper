
import logging
import json


from .publications import PublicationsSpider

logging.getLogger('asyncio').setLevel(logging.CRITICAL)



class Pub2Spider(PublicationsSpider):
    name = "pub2"
    allowed_domains = ["bw.sggw.edu.pl"]
    
    with open('data.json') as f:
            data = json.load(f)

    total_pages = data['publications_pages'][2:4]
    
