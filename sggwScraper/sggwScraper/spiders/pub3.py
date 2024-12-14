import logging
import json

from .publications import PublicationsSpider

logging.getLogger('asyncio').setLevel(logging.CRITICAL)

class Pub3Spider(PublicationsSpider):
    name = "pub3"
    allowed_domains = ["bw.sggw.edu.pl"]
    with open('data.json') as f:
            data = json.load(f)

    total_pages = data['publications_pages'][3:5]
