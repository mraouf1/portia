"""utils for the yaoota monitor"""

import requests
from urlparse import urljoin
import logging

from slybot.kipp_utils import utils

from scrapy.utils.project import get_project_settings


def new_crawl_iteration(merchant_id='', merchant_name='', scrapyd_job=''):
    """
    Create a new crawl iteration for the merchant.

    :param merchant_id: Merchant id
    :param merchant_name: Merchant name
    :return: None
    """

    new_crawl_iteration_api_endpoint = 'crawledproducts/merchant/%s/new_crawl_iteration/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    try:

        response = utils.rest_post(urljoin(url, new_crawl_iteration_api_endpoint % (merchant_id or merchant_name)),
                                   headers=headers,
                                   verify_ssl=verify_ssl,
                                   data={"merchant_id": merchant_id if merchant_id else merchant_name,
                                         "kipp_job": scrapyd_job})
        logging.info("Crawled iteration for merchant %s started", merchant_id or merchant_name)
        return response
    except RuntimeError as ex:
        logging.error('request could not be completed, the following exception occurred: %s', ex)


def finish_crawl_iteration(status='', merchant_name='', merchant_id='', stats=''):
    """
    Update the last crawl iteration for the merchant.

    :param merchant_id: Merchant id
    :param merchant_name: Merchant name
    :param status: crawling status
    :return: None
    """

    end_crawl_iteration_api_endpoint = 'crawledproducts/merchant/%s/end_crawl_iteration/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')
    # Convert stats start/end time to string
    if stats:
        # Convert the dates from datatime object to string to be able to jsonify it
        stats['start_time'] = stats['start_time'].strftime('%m/%d/%Y %H:%M:%S:%f')
        stats['finish_time'] = stats['finish_time'].strftime('%m/%d/%Y %H:%M:%S:%f')
    try:
        response = utils.rest_post(urljoin(url, end_crawl_iteration_api_endpoint % (merchant_id or merchant_name)),
                                   headers=headers,
                                   verify_ssl=verify_ssl,
                                   data={"status": status,
                                         "stats": stats})
        logging.info("Crawled iteration for merchant %s ended with status %s", (merchant_id or merchant_name), status)
        return response
    except RuntimeError as ex:
        logging.error('request could not be completed, the following exception occurred: %s', ex)


def get_merchant_latest_iteration(merchant_name='', merchant_id=''):
    """
    Get the merchant_latest_iteration
    :param merchant_name: Mechant name
    :param merchant_id: Merchant id
    :return:
    """
    get_latest_iteration_api_endpoint = 'crawledproducts/merchant/%s/get_latest_iteration/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    try:
        response = utils.rest_get(urljoin(url, get_latest_iteration_api_endpoint % (merchant_id or merchant_name)),

                                  headers=headers,
                                  verify_ssl=verify_ssl)
        logging.info("Getting the latest iteration for merchant %s", merchant_id if merchant_id else merchant_name)
        return response
    except RuntimeError as ex:
        logging.error('request could not be completed, the following exception occurred: %s', ex)
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            logging.error('request could not be completed, the following exception occurred: %s', e.message)
            return None
        else:
            raise
