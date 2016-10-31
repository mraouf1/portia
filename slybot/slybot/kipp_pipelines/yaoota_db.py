"""
Yaoota db pipeline
"""
import logging
from scrapy.exceptions import DropItem
from slybot.kipp_utils import utils, item_utils
from slybot.kipp_utils.utils import timing
import os

logger = logging.getLogger(__name__)


class YaootaDBPipeline(object):
    """
    Yaoota db pipeline
    features :
        - Do sanity check on the item ( Making sure it has all the basic fields for a yaoota crawled product )
        - Checking the items stock status
        - Making sure the items is consistent ( All it's fields in populated )
        - Adding item to yaoota db through yaoota API
        - Populate the crawler stats with some data that is used by other pipelines
    """

    def __init__(self):
        self.crawled_products_count = 0
        self.first_item_modified_date = None
        # common crawled products between the new and the previous crawl iteration
        self.survived_crawled_products = 0
        self.scrapyd_job = os.environ.get('SCRAPY_JOB', '')

    @classmethod
    def from_crawler(cls, crawler):
        try:
            pipe = cls.from_settings(crawler.settings)
        except AttributeError:
            pipe = cls()
        pipe.crawler = crawler
        return pipe

    @timing("YaootaDBPipeline", logging)
    def process_item(self, item, spider):
        # TODO : This need to be enhanced a bit
        # Making sure that the item is consistence for displaying in web interface
        # by making sure that *_en and *_ar keys are always populated

        # Disabling proxy & Saving proxy settings
        proxy_is_enabled = False
        proxy_config = utils.get_proxy_settings()
        http_proxy = proxy_config['http_proxy']
        https_proxy = proxy_config['https_proxy']
        if http_proxy or https_proxy:
            utils.disable_proxy()
            proxy_is_enabled = True

        item = item_utils.populate_empty_localized_attributes(item)
        # Adding item to yaoota db through the API
        logging.info("Saving item with remote id %s to the db", item['remote_id'])
        data = dict(item)
        data['scrapyd_job'] = self.scrapyd_job
        result = item_utils.save_item(item=data, merchant_name=spider.merchant_name)
        # Populating the 1st created crawled product object modified date

        self.crawled_products_count += 1
        self.survived_crawled_products += result['already_in_db']
        self.crawler.stats.set_value('yaoota/survived_crawled_products', self.survived_crawled_products)
        if proxy_is_enabled:
            utils.enable_proxy(http_proxy=http_proxy, https_proxy=https_proxy)
        return item
