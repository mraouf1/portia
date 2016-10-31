"""Kipp item utils responsible for scraping and cleaning item data."""
import re
import logging
from bs4 import BeautifulSoup
from urlparse import urljoin
from urlparse import urlparse, parse_qs, parse_qsl, ParseResult
from urllib import urlencode, unquote
from scrapy.utils.project import get_project_settings
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from scrapy.exceptions import DropItem
import html2text
from slybot.kipp_utils.utils import get_safe_html, timing
from slybot.kipp_utils.utils import listify, is_value_empty, substitute_regex, rest_post, rest_get

UNSTORABLE_ATTRIBUTES = ['seller', 'product_condition']


@timing("ItemUtils", logging)
def initialize_kipp_item(item_fields, kipp_item):
    """
    Set all attributes of kipp item with default values
    :param item_fields : kipp default fields
    :param kipp_item: kipp item object
    :return: dict -- kipp item dict
    """
    for attr in item_fields:
        kipp_item[attr] = ''
    return kipp_item


@timing("ItemUtils", logging)
def populate_item(item_fields, item, attr_name, value, unprocessed_value='', lang='en'):
    """
    Populate kipp item object
    :param item: Kipp item object
    :param attr_name: Attribute name. example : title
    :param value: Value of the attribute
    :return: None
    """
    # We need to make sure that the value is unicode or string
    # TODO: needs to be done more smarter
    if value:
        if type(value) in (unicode, str):
            localized_attr_name = "%s_%s" % (attr_name, lang)
            # check if we should populate the (non)localized name.
            if localized_attr_name in item_fields:
                if attr_name == 'description':
                    # populate description_html field using the unprocessed description scraped value
                    item['description_html_%s' % lang] = get_safe_html(unprocessed_value[0])
                elif attr_name == 'category':
                    # populate complete_category_lan using the category scraped value
                    item['complete_category_%s' % lang] = value
                attr_name = localized_attr_name

        # Special case for extra_images which is a list and extra_info which is a dict
        elif attr_name not in ('extra_images', 'extra_info'):
            msg = "Attribute %s 's type is %s" % (attr_name, type(value))
            logging.error(msg)
            raise AttributeError(msg)
        item[attr_name] = value

    else:
        logging.warning("Attention! value for attribute %s is not defined", attr_name)


@timing("ItemUtils", logging)
def is_seller_allowed(seller, sellerlist):
    """
    Check if seller is in sellers list.

    :param seller: scraped seller value.
    :param sellerlist: allowed sellers list.
    :return boolean -- true if seller is in seller list, false otherwise. (ignoring case)
    """
    sellerlist = [sel.lower() for sel in sellerlist]
    return seller.lower().strip() in sellerlist


@timing("ItemUtils", logging)
def is_condition_allowed(product_condition, allowedlist):
    """
    Check if scraped product condition is allowed.

    :param product_condition: scraped product condition value.
    :param allowedlist: allowed condition value list.
    """
    return product_condition.strip() in allowedlist


VALIDATOR_FUNCTIONS = {'seller': is_seller_allowed,
                       'product_condition': is_condition_allowed}


@timing("ItemUtils", logging)
def is_value_allowed(attr_name, scraped_value, attr_config):
    """
    Check if the value for attr_name is in the allowed list.

    :param attr_name: name of the attribute.
    :param scraped_value: value of the attribute.
    :param attr_config: attribute configs for the attribute.
    :return: boolean
    """
    if callable(VALIDATOR_FUNCTIONS.get(attr_name)):
        allowed_list = attr_config['allowed_list']
        return VALIDATOR_FUNCTIONS[attr_name](scraped_value[0], allowed_list)
    return True


@timing("ItemUtils", logging)
def soup_extract(tag_string, method_name, html_string):
    """
    Util for specifications callback functions.

    :param tag_string: Get the data surrounded by that tag.
    :param method_name: Function name to apply on the 'html_string'.
    :param html_string: Html string to get the tags specified by the 'tag_string'.
    :raises RuntimeError:
    :return: List 'tag_string' strings.
    """
    if not tag_string:
        raise RuntimeError("You must specify a tag_string.")
    if not method_name:
        raise RuntimeError("You must specify a function.")
    if not html_string:
        raise RuntimeError("Html string is not provided")

    from bs4 import BeautifulSoup

    document = BeautifulSoup(html_string)
    method = getattr(document, method_name)
    if not method or not callable(method):
        raise RuntimeError("Method '%s' is not attr in BeautifulSoup" % method_name)

    return method(tag_string)


@timing("ItemUtils", logging)
def process_category(scraped_value, params={}):
    """
    Process scraped category value.

    :param scraped_value: string -- scraped category value.
    :param params: dict -- extra params
    :return: string -- processed category value.
    """
    if 'max_category_level' in params:
        category = "\\".join([element.strip().lower() for element in scraped_value[0:params['max_category_level']]])
    else:
        category = "\\".join([element.strip().lower() for element in scraped_value])
    if not category:
        msg = "Category is not defined"
        logging.error(msg)
        raise AttributeError(msg)
    return category


@timing("ItemUtils", logging)
def _clean_description(description):
    cleaner = html2text.HTML2Text()
    cleaner.ignore_links = True
    cleaner.ignore_images = True
    return cleaner.handle(description).strip()


@timing("ItemUtils", logging)
def process_description(scraped_value, params={}):
    """
    Process scraped description value.
    This function takes an HTML description
    and returns it with HTML tags escaped.

    :param scraped_value: string -- scraped description value.
    :param params: dict -- extra params
    :return: string -- processed description value.
    """

    if len(scraped_value) > 1:
        logging.warning("Attention!, product description has more than one element")

    return _clean_description(scraped_value[0])


@timing("ItemUtils", logging)
def process_price(scraped_value, params={}):
    """
    Process scraped price value.
    This function removes invalid characters from a number and returns it.
    If the argument is a list, the minimum value will be returned.

    :param scraped_value: string --  scraped price value.
    :param params -- extra params
    :return: string -- processed price value.

    """

    def doesnt_contain_text(value):
        return not bool(re.search(r'[a-zA-Z]+', value))

    if not scraped_value:
        return None
    prices_list = [float(re.sub(r',', '', value)) for value in scraped_value if value and doesnt_contain_text(value)]
    if prices_list:
        price = min(prices_list)
        return str(price)
    else:
        raise RuntimeError("to-be-processed price is not defined")


PROCESSOR_FUNCTIONS = {'price': process_price,
                       'description': process_description,
                       'category': process_category}


@timing("ItemUtils", logging)
def apply_attribute_processor(attr_name, value, params={}):
    """
    Apply processing function on attribute value.
    If attribute has not processing function, then value will be returned.

    :param attr_name: string -- name of the attribute.
    :param value: string -- value of the attribute.
    :param params: dict -- extra params
    """
    return PROCESSOR_FUNCTIONS[attr_name](value, params=params) if callable(
            PROCESSOR_FUNCTIONS.get(attr_name)) else value


@timing("ItemUtils", logging)
def should_skip_item(value, attr_config):
    """
    Returns true if value is empty and value is required.

    :param value: value to be checked.
    :param is_required: boolean value indicating of value is required.
    :return: boolean.
    """
    return is_value_empty(value) and attr_config.get('is_required')


@timing("ItemUtils", logging)
def apply_callback_function(value, attr_config):
    """
    Apply callback function on value and return result.
    If no callback function is available, then value will be returned.

    :param list
    :param dict: attribute config
    :return: object -- output of the callback function, or the value arg.
    """

    def helper(value, attr_config):
        """
        Apply callback helper function. Takes a value and attr_config,
        and calls callback function on the value and returns the result.
        If no callback function is available, then value will be returned.

        :param list
        :param dict: attribute config
        :return: object -- output of the callback function, or the value arg.
        """
        return attr_config['callback'](value) if callable(attr_config.get('callback')) else value

    if attr_config.get('configs'):
        return [(pair[0], helper(pair[1], attr_config['configs'][pair[0]])) for pair in value]
    return helper(value, attr_config)


@timing("ItemUtils", logging)
def apply_subquery_regex(value, attr_config):
    """
    Apply regex substitutions of available on value.
    If value is a list, substitutions will be applied
    on all elements of the list.

    :param object -- a string or list of strings.
    :param dict -- attribute configs
    :return: object -- a string or a list of strings/tuples after applying substitutions
    """

    def helper(value, config):
        """
        Apply regex substitutions of available on value.
        If value is a list, substitutions will be applied
        on all elements of the list.

        :param object -- a string or list of strings.
        :param dict -- attribute configs containing the sub_regex settings.
        :return: object -- a string or a list of strings after applying substitutions
        """
        if config.get('sub_query_regex'):
            if type(value) == list:
                return [substitute_regex(element.strip(), config['sub_query_regex'],
                                         config['sub_query_string']).strip() for element in value]
            return substitute_regex(value, config['sub_query_regex'], config['sub_query_string'])
        return value

    if attr_config.get('configs'):
        return [(pair[0], helper(pair[1], attr_config['configs'][pair[0]])) for pair in value]
    return helper(value, attr_config)


@timing("ItemUtils", logging)
def scrape_attribute_value(response, attr_config):
    """
    Scrape a product's attribute value.

    :param response: response from webrequest.
    :param attr_config: attribute's parsing config.
    :return: list -- product attribute value
    """

    def scrape_helper(config):
        """
        Parse attribute data from the response passed in to the outer function.

        :param config: attribute's parsing config.
        :return: list -- product attribute value
        """
        if not hasattr(response, 'css'):
            return None

        attr_query = response
        # FIXME: better to generalize this.
        # if an alternative parser should be used?
        if config.get('alternative_parser'):
            # construct a beautifulsoup doc from the response body using native python html parser.
            doc = BeautifulSoup(response.body, 'html.parser')
            # contruct a selector object from the document.
            attr_query = Selector(text=str(doc))
        # Checks if the item has a css query defined in the configuration else we will skip it
        if not config.get('xpath_query') and not config.get('css_query'):
            return None
        if config.get('css_query'):
            attr_query = attr_query.css(config['css_query'])
        if config.get('xpath_query'):
            attr_query = attr_query.xpath(config['xpath_query'])
            if config.get('xpath_regex'):
                return attr_query.re(config['xpath_regex'])
        return attr_query.extract()

    if attr_config.get('configs'):
        return [(pair[0], scrape_helper(pair[1])) for pair in attr_config['configs'].iteritems()]
    return scrape_helper(attr_config)


@timing("ItemUtils", logging)
def populate_images(item, local_images=False, merchant_url_config=''):
    """
    Populate image_urls for the passed item.
    If operation was successful,
    True is returned, otherwise False is returned.

    :param item: kippItem object.
    :param local_images: true if image urls are relative urls.
    :param  merchant_url_config: Merchant url configuration
    :return: boolean
    """

    merchant_urls_list = [url_config['url'] for url_config in merchant_url_config]
    if item['main_image']:
        if type(item['main_image']) is list and len(item['main_image']) > 1:
            logging.error("Main image is a list of more than 1 image for item with url %s.It should be only 1 image",
                          item['url_en'])

    # If all images have the same css/xpath selectors
    elif item['extra_images']:
        item['extra_images'] = filter(None, item['extra_images'])
        item['extra_images'] = listify(item['extra_images'])
        # because sometimes the extra_images are parsed as a list of empty strings ex:[u'' , u''] so we must...
        # check if list is still not empty
        # if item['extra_images']:
        if item['extra_images']:
            item['main_image'] = item['extra_images'].pop(0)

    if local_images:
        if item['main_image']:
            item['main_image'] = urljoin(merchant_urls_list[0], item['main_image'])
        if item['extra_images']:
            item['extra_images'] = map(lambda extra_image: urljoin(merchant_urls_list[0], extra_image),
                                       listify(item['extra_images']))

    all_product_images = (listify(item['main_image'] if item['main_image'] else []) + (
        listify(item['extra_images']) if item['extra_images'] else []))

    # to clean the extra images from empty elements
    all_product_images = filter(None, all_product_images)

    if not all_product_images:
        return False

    item['image_urls'] = list(set([product_image for product_image in all_product_images if
                                   product_image not in merchant_urls_list]))

    return True


@timing("ItemUtils", logging)
def cleanup(scraped_value, attr_config):
    """
    Perform final clean up on a scraped value before storing it in a KippItem.

    :param scraped_value: scraped value from web response.
    :return: object: scraped value after cleanup.
    """

    def helper(value):
        """
        cleanup helper function.

        :param value: scraped value as a list or string
        :return: list or string -- value after cleanup.
        """
        if type(value) == list:
            if len(value) == 1:
                return value[0].strip()
        elif type(value) in (str, unicode):
            return value.strip()
        return value

    # extra_attributes case
    if attr_config.get('configs'):
        return [(pair[0], helper(pair[1])) for pair in scraped_value if not is_value_empty(pair[1])]
    # other cases
    return helper(scraped_value)


@timing("ItemUtils", logging)
def get_stock_status(scraped_value, attr_config):
    """
    Get stock status of the scraped product.

    :param scraped_value: stock status.
    :param attr_config: attr_config of stock status as a dict.
    :return: string -- stock status or None if stock_status arg is empty.
    """
    if not scraped_value:
        return None
    # Checking if the attribute have list of instock values to check if the scraped value in it and return IN_STOCK.
    elif 'instock_values' in attr_config and any(
            [unicode(value) in attr_config['instock_values'] for value in scraped_value]):
        return 'IN_STOCK'
    # Checking if the attribute have list of outstock values to check if the scraped value in it and return OUT_OF_STOCK.
    elif 'outstock_values' in attr_config and any(
            [unicode(value) in attr_config['outstock_values'] for value in scraped_value]):
        return 'OUT_OF_STOCK'
    # if the attribute neither IN_STOCK nor OUT_OF_STOCK.
    return None


@timing("ItemUtils", logging)
def merge_items(current_item, new_item):
    """
    Merge two items
    :param current_item: item - kipp item
    :param new_item: item - kipp item
    :return: item - kipp item
    """
    keys_to_ignore = ('stock_status', 'url_en', 'url_ar')
    current_item.update(
            {key: value for key, value in new_item.iteritems() if value and key not in keys_to_ignore})
    return current_item


def get_value_from_config(response, attr_config):
    scraped_value = scrape_attribute_value(response, attr_config)
    scraped_value = apply_callback_function(scraped_value, attr_config)
    return scraped_value


def get_product_url(response, product_config):
    """
    Tries to get the url value using configuration, returns response.url otherwise.
    :param response:
    :param product_config:
    :return:
    """
    url_config = filter(lambda config: config[0] == 'url', product_config)
    if url_config:
        url_config = url_config[0]
        url = get_value_from_config(response, url_config[1])
        if url:
            return url
    return response.url


@timing("ItemUtils", logging)
def parse_item(spider, response, lang, ignore_list=()):
    """
    Parse data from a web response and return a KippItem.
    :param spider: scrapy Spider object used in crawling.
    :param response: web response from spider.
    :param ignore_list: the list of ignored attributes EX: price in arabic items
    :param lang: the language of the URL.
    :return dict -- KippItem.
    """
    from kipp_base.items import KippItem

    if spider.shut_down:
        raise CloseSpider(reason='An instance of the spider for merchant %s is already running' % spider.merchant_name)
    item = KippItem()
    # Setting kipp item to default
    initialize_kipp_item(item_fields=KippItem.fields,
                         kipp_item=item)
    product_url = get_product_url(response, spider.product_items_config)

    is_valid_item = True
    for attr_name, attr_config in spider.product_items_config:
        if attr_name in ignore_list or lang == 'ar' and attr_config.get('meta_attribute', False):
            logging.info("Value of attribute %s is ignored", attr_name)
            continue
        logging.info("Beginning to scrape and process %s", attr_name)
        if attr_config.get('meta_attribute', False):
            scraped_value = response.meta.get('referer_%s' % attr_name)
            logging.info("Getting attribute %s from meta instead of response, value is %s", attr_name, scraped_value)
        else:
            scraped_value = get_value_from_config(response, attr_config)

        logging.info("Value of attribute %s after applying call back method is %s", attr_name, scraped_value)
        # check if we should skip the item.
        if should_skip_item(scraped_value, attr_config):
            # Checking if the item out of stock and the current attribute have default value to set it.
            if item['stock_status'] == 'OUT_OF_STOCK' and 'default_value' in attr_config:
                scraped_value = attr_config['default_value']
            else:  # Item is out of stock and no default value for the attribute.
                logging.warning("Item at url %s is not valid because it is missing the %s attribute", product_url,
                                attr_name)

                is_valid_item = False
                break
        # check if scraped value is empty.
        if is_value_empty(scraped_value):
            logging.warning("Item at url %s, attribute %s is not parse-able. Skipping.", product_url, attr_name)

            continue

        # apply regex substitutions, if available.
        scraped_value = apply_subquery_regex(scraped_value, attr_config)
        # if attribute has a processing function, then call it
        # construct the params
        params = {'max_category_level': spider.category_max_level, 'item': item}
        # scraped value without applying the attribute processor will be needed later to populate description_html's field
        unprocessed_value = scraped_value
        scraped_value = apply_attribute_processor(attr_name, scraped_value, params=params)
        logging.info('%s value after attribute processing is %s', attr_name, scraped_value)
        # if attribute has a validation function, then call it.
        is_value_valid = is_value_allowed(attr_name, scraped_value, attr_config)
        if not is_value_valid:
            is_valid_item = False
            logging.info("Item at url %s is not valid because %s is not in the allowed list", product_url,
                         scraped_value)
            break
        # Because we don't store things like product condition and seller.
        if attr_name in UNSTORABLE_ATTRIBUTES:
            continue
        # special case
        if attr_name == 'stock_status':
            stock_status = get_stock_status(scraped_value, attr_config)
            if not stock_status:
                logging.warning("Could not scrape stock status for item at url %s, item will be skipped.", product_url)
                is_valid_item = False
                break
            else:
                scraped_value = stock_status

        # final cleanup of a scraped value.
        attr_value = cleanup(scraped_value, attr_config)
        logging.info('%s value after cleanup is %s', attr_name, attr_value)
        populate_item(item_fields=KippItem.fields,
                      item=item,
                      attr_name=attr_name,
                      value=attr_value, unprocessed_value=unprocessed_value, lang=lang)

    if not is_valid_item:
        # Filtering the non-valid products
        spider.invalid_items_counter += 1
        return None
    # TODO: Need to populate url localization based on an intelligent detection
    # Populate the url

    item['url_en'] = product_url
    if lang != 'ar':
        # Populating the image_urls list for images download and conversion.
        is_images_populated = populate_images(item, spider.local_images, spider.urls_config)
        if not is_images_populated:
            logging.error("Could not populate images at %s", product_url)
    return item


@timing("ItemUtils", logging)
def save_item(item, merchant_id='', merchant_name=''):
    """
    Save item in the datebase
    :param item: item info as a dict
    :param merchant_id: merchant id
    :param merchant_name: merchant name
    :return None
    :raises RuntimeError
    """
    crawled_product_api_endpoint = 'crawledproducts/merchant/%s/add_crawled_product/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        crawled_product_api_endpoint = crawled_product_api_endpoint % merchant_id
    elif merchant_name:
        crawled_product_api_endpoint = crawled_product_api_endpoint % merchant_name

    else:
        raise RuntimeError("Merchant id or name is not found")

    return rest_post(url=urljoin(url, crawled_product_api_endpoint), verify_ssl=verify_ssl,
                     headers=headers, data=item)


@timing("ItemUtils", logging)
def remove_deleted_products(urls, merchant_id='', merchant_name=''):
    """
    remove products' urls from the datebase
    :param urls: urls to be deleted from the database
    :param merchant_id: merchant id
    :param merchant_name: merchant name
    :return None
    :raises RuntimeError
    """
    deleted_products_api_endpoint = 'crawledproducts/merchant/%s/delete_out_of_stock_list/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        deleted_products_api_endpoint = deleted_products_api_endpoint % merchant_id
    elif merchant_name:
        deleted_products_api_endpoint = deleted_products_api_endpoint % merchant_name

    else:
        raise RuntimeError("Merchant id or name is not found")

    return rest_post(url=urljoin(url, deleted_products_api_endpoint), verify_ssl=verify_ssl,
                     headers=headers, data={'products_urls': urls})


@timing("ItemUtils", logging)
def delete_out_of_stock_item(remote_id, merchant_id='', merchant_name=''):
    """
    Delete out of stock items from the web database.
    :param remote_id: item remote id
    :param merchant_id: merchant id
    :param merchant_name: merchant name
    :return None
    :raises RuntimeError
    """
    delete_out_stock_api_endpoint = 'crawledproducts/merchant/%s/delete_out_of_stock/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        delete_out_stock_api_endpoint = delete_out_stock_api_endpoint % merchant_id
    elif merchant_name:
        delete_out_stock_api_endpoint = delete_out_stock_api_endpoint % merchant_name
    else:
        raise RuntimeError("Merchant id or name is not found")

    return rest_post(url=urljoin(url, delete_out_stock_api_endpoint), verify_ssl=verify_ssl,
                     headers=headers, data={'remote_id': remote_id})


@timing("ItemUtils", logging)
def cleanup_deleted_items(merchant_id='', merchant_name='', scrapyd_job=''):
    """
    clean items ( product ) that didn't appear in the current crawl iteration
    :param merchant_id: Merchant id
    :param merchant_name: Merchant name
    :param scrapyd_job: the scrapyd_job for the current iteration
    :return:
    """
    clean_crawled_product_api_endpoint = 'crawledproducts/merchant/%s/clean_crawled_products/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        clean_crawled_product_api_endpoint = clean_crawled_product_api_endpoint % merchant_id
    elif merchant_name:
        clean_crawled_product_api_endpoint = clean_crawled_product_api_endpoint % merchant_name

    else:
        raise RuntimeError("Merchant id or name is not found")

    return rest_post(url=urljoin(url, clean_crawled_product_api_endpoint), verify_ssl=verify_ssl,
                     headers=headers, data={'scrapyd_job': scrapyd_job})


@timing("ItemUtils", logging)
def get_products_count(merchant_id='', merchant_name=''):
    """
    Get the products count for certain merchant ( which has a flag is_deleted=False)
    :param merchant_id: Merchant id
    :param merchant_name: Merchant name
    :return:
    """
    get_products_count_endpoint = 'crawledproducts/merchant/%s/count/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        get_products_count_endpoint = get_products_count_endpoint % merchant_id
    elif merchant_name:
        get_products_count_endpoint = get_products_count_endpoint % merchant_name

    else:
        raise RuntimeError("Merchant id or name is not found")

    data = rest_get(url=urljoin(url, get_products_count_endpoint), verify_ssl=verify_ssl,
                    headers=headers)
    if 'count' not in data:
        raise RuntimeError("Cannot find count in the response")
    return int(data['count'])


@timing("ItemUtils", logging)
def is_item_valid(item, spider=None):
    """
    Do sanity check on item that it has the basic keys for a valid product
    Keys are title_en or title ar, remote_id, stock_status, price_en or price_ar, main_images, extra_images
    :param item: crawled product as a dict
    :param spider: the current running spider to update some values in it
    :return: bool - if item is okay or not
    """
    # Every key is a tuple contain the localization of the key if valid
    keys = [('title_en', 'title_ar'), ('price_en', 'price_ar'), ('main_image', 'extra_images'),
            ('stock_status',), ('remote_id',), ('category_ar', 'category_en')]
    # Checks the url
    if not item['url_en']:
        logging.error("Attention! item %s has no url key. please check the other keys as well", item)
        return False
    for keys_tuple in keys:
        keys_found = []
        for key in keys_tuple:
            if key in item and item[key]:
                keys_found.append(True)
            else:
                keys_found.append(False)
        if not any(keys_found):
            logging.error(
                    "Attention! Item with url %s is not valid because it's missing "
                    "one of %s keys or they are not populated. Please check other keys as well",
                    item['url_en'],
                    keys_tuple)
            return False
    # Check if item is in stock
    if item['stock_status'] != 'IN_STOCK':
        merchant_name = 'NA'
        if spider:
            spider.out_of_stock_items_counter += 1
            merchant_name = spider.merchant_name
        remote_id = item['remote_id']
        logging.warning("Product with id %s is not in-stock" % remote_id)
        if spider.is_delete_out_of_stock:
            logging.info("Calling the out_of_stock_api with merchant %s and remote_id %s" % (merchant_name, remote_id))
            try:
                delete_out_of_stock_item(remote_id, merchant_name=merchant_name)
            except:
                logging.error("Can't call delete_out_of_stock API")
        return False
    else:
        if spider:
            spider.in_stock_items_counter += 1
            if not item['manufacturer_en']:
                spider.manufacturer_empty_items_counter += 1
            else:
                spider.manufacturer_populated_items_counter += 1
    price = item.get("price_en", '') or item.get("price_ar", '')

    if float(price) in (0.0, 0):
        logging.warning("Product with id %s and url %s has 0 price" % (item['remote_id'], item['url_en']))
        if spider:
            spider.zero_price_counter += 1
        return False
    if item['model']:
        if spider:
            spider.model_populated_items_counter += 1
    return True


@timing("ItemUtils", logging)
def populate_empty_localized_attributes(item):
    """
    Make sure to populate all empty localized attributes. i.e *_ar , *_en
    so the idea is to populate x_en with x_ar if empty and vice versa
    :param item: dict -- kipp item
    :return: dict -- populate item
    """
    english = '_en'
    arabic = '_ar'
    for attribute_name, attribute_value in item.copy().iteritems():
        if english in attribute_name:
            attr_name_not_localized = attribute_name.replace(english, '')
            arabic_localized_name = attr_name_not_localized + arabic
            # If english attribute is not populated. we will populate it with the arabic one
            if not attribute_value:
                item[attribute_name] = item.get(arabic_localized_name, [''])
            # Else viceversa. populate arabic attribute with the english one
            elif not item.get(arabic_localized_name):
                item[arabic_localized_name] = item[attribute_name]
    return item


@timing("ItemUtils", logging)
def get_merchant_details(merchant_name='', merchant_id=''):
    """
    Get the merchant detail from the db using REST api
    :param merchant_name: string
    :param merchant_id : int
    :return: dict - merchant details
    """
    get_merchant_details_endpoint = 'stores/%s/'
    project_settings = get_project_settings()
    url = project_settings.get('YAOOTA_API_URL')
    headers = project_settings.get('YAOOTA_API_HEADERS')
    verify_ssl = project_settings.get('YAOOTA_API_SSL_VERIFICATION')

    if not url:
        raise RuntimeError("Yaoota API url is not found")

    if merchant_id:
        get_merchant_details_endpoint = get_merchant_details_endpoint % merchant_id
    elif merchant_name:
        get_merchant_details_endpoint = get_merchant_details_endpoint % merchant_name

    else:
        raise RuntimeError("Merchant id or name is not found")

    data = rest_get(url=urljoin(url, get_merchant_details_endpoint), verify_ssl=verify_ssl,
                    headers=headers)

    return data


@timing("ItemUtils", logging)
def add_url_params(url, params):
    """ Add GET params to provided URL being aware of existing.

    :param url: string of target URL
    :param params: string containing requested params to be added
    :return: string with updated URL

    >> url = 'http://stackoverflow.com/test?answers=true'
    >> new_params = {'answers': False, 'data': ['some','values']}
    >> add_url_params(url, new_params)
    'http://stackoverflow.com/test?data=some&data=values&answers=false'
    """
    # Unquoting URL first so we don't loose existing args
    url = unquote(url)
    # Extracting url info
    parsed_url = urlparse(url)
    # Extracting URL arguments from parsed URL
    get_args = parsed_url.query
    # Converting URL arguments to dict
    parsed_get_args = dict(parse_qsl(get_args))
    # Merging URL arguments dict with new params
    parsed_get_args.update(parse_qs(params))
    # Converting URL argument to proper query string
    encoded_get_args = urlencode(parsed_get_args, doseq=True)
    # Creating new parsed result object based on provided with new
    # URL arguments. Same thing happens inside of urlparse.
    new_url = ParseResult(
            parsed_url.scheme, parsed_url.netloc, parsed_url.path,
            parsed_url.params, encoded_get_args, parsed_url.fragment
    ).geturl()

    return new_url
