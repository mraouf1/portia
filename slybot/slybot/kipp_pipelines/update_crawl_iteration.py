"""
Update crawl iteration pipeline
"""
import logging
import copy
import os
from scrapy.utils.project import get_project_settings
from scrapyd_api import ScrapydAPI
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

from slybot.kipp_utils import utils, item_utils, monitor_utils
from slybot.kipp_utils.utils import timing

logger = logging.getLogger(__name__)

ZERO_PRICE_PERCENTAGE_THRESHOLD = 30


class UpdateCrawlIteration(object):
    """
    Update crawl iteration class
    Features:
      - Check if there's an instance running from this spider
      - Add new crawl iteration when the spider open
      - Report number of errors that happened during the iteration
      - Delete non-updated items using the web API
      - End the current iteration when the spider close
    """

    def __init__(self):
        """
        init method
        :return:
        """
        self.products_deletion_percentage = 25
        self.merchant_name = ''
        self.current_crawled_products_db_count = 0
        self.scrapyd_job = os.environ.get('SCRAPY_JOB', '')
        dispatcher.connect(self.spider_open, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler):
        try:
            pipe = cls.from_settings(crawler.settings)
        except AttributeError:
            pipe = cls()
        pipe.crawler = crawler
        return pipe

    def process_item(self, item, spider):
        """
        Process item
        :param item: kipp item
        :param spider: spider object
        :return:
        """
        return item

    @timing("UpdateCrawlIteration", logging)
    def _start_crawl_iteration(self):
        logging.info("Starting spider for merchant %s", self.merchant_name)
        self.current_crawled_products_db_count = item_utils.get_products_count(merchant_name=self.merchant_name)
        logging.info('Current crawled products db count is %s', self.current_crawled_products_db_count)
        monitor_utils.new_crawl_iteration(merchant_name=self.merchant_name, scrapyd_job=self.scrapyd_job)

    @timing("UpdateCrawlIteration", logging)
    def _get_job_status(self, scrapyd_job):
        """
        :param scrapyd_job: string indicating scrapyd job's id
        :return: string indicating its status which will be: 'running', 'pending', 'finished' or ''
        """
        project_settings = get_project_settings()
        project_name = 'kipp_base'
        scrapyd_url = project_settings.get('SCRAPYD_URL')
        scrapyd_api = ScrapydAPI(scrapyd_url)
        return scrapyd_api.job_status(project_name, scrapyd_job)

    @timing("UpdateCrawlIteration", logging)
    def _get_iteration_status(self, iteration_info):
        """
        :param iteration_info:
        :return: string inidcating the iteration status, it will: 'crawling', finished, 'killed'
        crawling: if the last crawl iteration didn't finish
        killed: means the merchant is not running but it stopped without finishing its crawl iteration
        finished: means that the merchant could finish its crawl iteration
        """
        if iteration_info and iteration_info['status'] == u'CRAWLING':
            kipp_job = iteration_info.get('kipp_job', '')
            if not kipp_job:
                return 'running'
            job_status = self._get_job_status(kipp_job)
            if job_status == 'finished':
                return 'killed'
            elif job_status == 'running':
                return 'crawling'
            else:
                return 'finished'
        else:
            return 'finished'

    def spider_open(self, spider):
        """
        overriding the spider open method
        :param spider: object
        :return:None
        """
        # Disabling proxy
        # Saving proxy settings
        proxy_is_enabled = False
        proxy_config = utils.get_proxy_settings()
        http_proxy = proxy_config['http_proxy']
        https_proxy = proxy_config['https_proxy']
        if http_proxy or https_proxy:
            utils.disable_proxy()
            proxy_is_enabled = True
        logging.info("Pipeline.spider_open called")
        self.merchant_name = spider.merchant_name
        # Check if merchant is already crawling
        iteration_info = monitor_utils.get_merchant_latest_iteration(merchant_name=self.merchant_name)
        iteration_status = self._get_iteration_status(iteration_info)
        logging.info("The last iteration status is %s", iteration_status)
        if iteration_status == 'running':
            spider.shut_down = True
        else:
            if iteration_status == 'killed':
                # this means the merchant didn't finish in expected way
                logging.info("The last iteration was killed, going to finish it")
                # finish the crawl iteration
                monitor_utils.finish_crawl_iteration(merchant_name=self.merchant_name, status='error')
            self._start_crawl_iteration()
            # Enabling proxy
            if proxy_is_enabled:
                utils.enable_proxy(http_proxy=http_proxy, https_proxy=https_proxy)

    def spider_closed(self, spider):
        """
        This method will be called when the spider is closed
        This method will be very helpful in case of marking the deleted items ( is_deleted = True ) ...etc
        :param spider: spider object
        :return: None
        """
        if not spider.shut_down:
            # Clean crawled products
            # Check if the deletion threshold is configured on the merchant settings level else use default value
            if spider.deletion_threshold:
                self.products_deletion_percentage = spider.deletion_threshold
            logging.info("Deletion threshold is %s", self.products_deletion_percentage)
            # Using the stats from the crawler engine to get the crawled products count during the crawl and

            survived_crawled_products = int(self.crawler.stats.get_value('yaoota/survived_crawled_products', 0))
            scraped_crawled_products = int(self.crawler.stats.get_value('item_scraped_count', 0))

            if self.current_crawled_products_db_count:
                # Checking the percentage of zero price items to log error.
                zero_price_percentage = (
                                            float(
                                                spider.zero_price_counter) / self.current_crawled_products_db_count) * 100
                if zero_price_percentage > ZERO_PRICE_PERCENTAGE_THRESHOLD:
                    logging.error("More than %s percent (%s) from products have 0 price",
                                  ZERO_PRICE_PERCENTAGE_THRESHOLD, zero_price_percentage)

            crawled_products_diff = self.current_crawled_products_db_count - survived_crawled_products
            logging.info("Crawled product difference between data in db and crawled data in this iteration is %s",
                         crawled_products_diff)
            # Check if the this is the merchant's 1st run ever
            merchant_1st_run_marker_found = utils.merchant_1st_run_marker_found(merchant_name=self.merchant_name)
            # flag that indicates if the scraped products are too low is set to True if the number of scraped products is zero or the deletion percentage is large
            scraped_few_items = scraped_crawled_products == 0
            if crawled_products_diff:
                # If the percentage of the delete product in the current crawl>X percentage of the existing crawled products
                # we won't cleanup the crawled products
                if self.current_crawled_products_db_count and merchant_1st_run_marker_found:
                    if ((crawled_products_diff * 100.0) / self.current_crawled_products_db_count) \
                            >= self.products_deletion_percentage:
                        logging.info(
                            "Won't cleanup deleted products. Deleted product exceeded the threshold %s",
                            self.products_deletion_percentage)
                        scraped_few_items = True
                    else:
                        logging.info("Cleaning up the deleted products")
                        item_utils.cleanup_deleted_items(merchant_name=self.merchant_name,
                                                         scrapyd_job=self.scrapyd_job)
                # We won't delete if the scraped is 0 !
                elif scraped_crawled_products:
                    logging.info("Deleting all old items from the db")
                    item_utils.cleanup_deleted_items(merchant_name=self.merchant_name,
                                                     scrapyd_job=self.scrapyd_job)
                    # Write the merchant 1st run marker
                    utils.write_merchant_1st_run_marker(merchant_name=self.merchant_name)
            iteration_status = 'finished'
            logging.info("Spider is in shutting down state")
            # End the crawled iteration
            # Adding more stats to the stats dict
            self.crawler.stats.set_value("out_stock_items_count", spider.out_of_stock_items_counter)
            self.crawler.stats.set_value("in_stock_items_count", spider.in_stock_items_counter)
            self.crawler.stats.set_value("invalid_items_count", spider.invalid_items_counter)
            self.crawler.stats.set_value("zero_price_items_count", spider.zero_price_counter)
            self.crawler.stats.set_value("manufacturer_populated_items_count",
                                         spider.manufacturer_populated_items_counter)
            self.crawler.stats.set_value("manufacturer_empty_items_count", spider.manufacturer_empty_items_counter)
            self.crawler.stats.set_value("model_populated_items_count", spider.model_populated_items_counter)
            self.crawler.stats.set_value("scraped_few_items", scraped_few_items)
            # Calculating iteration duration.
            start_time = self.crawler.stats.get_value('start_time', None)
            finish_time = self.crawler.stats.get_value('finish_time', None)
            if start_time and finish_time:
                self.crawler.stats.set_value("iteration_duration",
                                             round((finish_time - start_time).total_seconds() / 60.0 / 60.0, 2))

            stats = copy.deepcopy(self.crawler.stats.get_stats())
            monitor_utils.finish_crawl_iteration(merchant_name=self.merchant_name,
                                                 status=iteration_status,
                                                 stats=stats)
            self.notify_sentry(spider)

    def notify_sentry(self, spider):
        """
        Checks for two possible problems that could've happened regarding the current crawl iteration
        and constructs the messages list to be sent to sentry
        :param spider:
        :return: None
        """
        stats = self.crawler.stats
        country = spider.country
        messages = []
        scraped_few_items = stats.get_value("scraped_few_items")
        if scraped_few_items:
            scraped_products = stats.get_value('item_scraped_count', 0)
            messages.append(
                "[{country}-{merchant_name}] Scraped few items: {scraped_items}".format(country=country.upper(),
                                                                                        merchant_name=spider.merchant_name,
                                                                                        scraped_items=scraped_products))

        errors_count = stats.get_value("log_count/ERROR", 0)
        if errors_count:
            stats_dict = stats.get_stats()
            well_formated_stats = "{" + "\n".join("{}: {}".format(k, v) for k, v in stats_dict.items()) + "}"
            message = "[{country}-{merchant_name}] {errors_count} errors detected. here's the stats \n {stats} Please check this log for more info.".format(
                country=country.upper(), merchant_name=spider.merchant_name, errors_count=errors_count,
                stats=well_formated_stats)
            messages.append(message)
            logging.info(message)
        utils.notify_sentry(messages)
