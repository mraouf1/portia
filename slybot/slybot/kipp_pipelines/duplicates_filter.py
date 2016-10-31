"""
Duplicates filter pipeline
"""
import logging

from scrapy.exceptions import DropItem
from slybot.kipp_utils.utils import timing
logger = logging.getLogger(__name__)


class DuplicatesPipeline(object):
    """
    Duplicate items pipeline
    features :
      - Check the item's remote id type
      - Drop item is it's scraped before
    """
    def __init__(self):
        self.ids_seen = set()

    @timing("DuplicatesPipeline", logging)
    def process_item(self, item, spider):
        item_remote_id = item['remote_id'][0]
        if type(item_remote_id) not in (str, unicode):
            error_message = "Item remote id is not str or unicode instead the type is %s and value is %s and url %s" % (
                type(
                    item_remote_id), item_remote_id, item['url_en'][0])
            logging.error(error_message)
            raise DropItem("Item is invalid. Reason: %s" % error_message)
        elif item['remote_id'][0] in self.ids_seen:
            raise DropItem("Item with remote id %s is duplicate on url %s" % (item['remote_id'][0], item['url_en'][0]))
        else:
            self.ids_seen.add(item['remote_id'][0])
        return item
