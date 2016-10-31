from slybot.kipp_utils.item_utils import is_item_valid
from slybot.kipp_utils.utils import timing
from scrapy.exceptions import DropItem
import logging
logger = logging.getLogger(__name__)

class ItemValidationPipeline(object):
    @timing("ItemValidationPipeline", logging)
    def process_item(self, item, spider):
        # Do sanity check on the item
        if not is_item_valid(item, spider):
            raise DropItem("Item is not valid")
        return item
