"""
Common util functions
"""
import os
import langid
import requests
import re
import json
import logging
import slackpy
import bleach
import psutil
import time

from urlparse import urlsplit, urlunsplit, parse_qsl, SplitResult
from urllib import urlencode
from xml.etree import ElementTree as ET
from raven import Client as sentry_client

from scrapy.utils.project import get_project_settings


def timing(class_name='', logging_obj=None):
    def timing_doc(f):
        def wrap(*args, **kwargs):
            time1 = time.time()
            ret = f(*args, **kwargs)
            time2 = time.time()
            if logging_obj:
                logging_obj.info('%s.%s function took %0.3f s' % (class_name, f.func_name, (time2 - time1)))
            else:
                logging.info('%s.%s function took %0.3f s' % (class_name, f.func_name, (time2 - time1)))
            return ret

        return wrap

    return timing_doc


def detect_lang(string):
    """
    Detect the language of a certain string
    :param string: The string which we will detect language for
    :return: string -- language
    """
    # Set the language boundaries
    langid.set_languages(['ar', 'en'])
    if not string:
        logging.warning("Can not detect language. String is empty")
    return langid.classify(string)[0]


def enable_proxy(http_proxy, https_proxy):
    """
    Enable proxy by populating the http_proxy and https_proxy env. variable

    :param http_proxy: http proxy address
    :param https_proxy: https proxy address
    :return:
    """
    if http_proxy == '' and https_proxy == '':
        logging.warning("enable_proxy must have atleast one http")
        return
    if is_valid_url(http_proxy):
        os.environ['http_proxy'] = http_proxy
    else:
        os.environ['http_proxy'] = ''
    if is_valid_url(https_proxy):
        os.environ['https_proxy'] = https_proxy
    else:
        os.environ['https_proxy'] = ''


def disable_proxy():
    """
    Disable proxy by populating the http_proxy and https_proxy env. variable with empty string
    :return: None
    """
    os.environ['http_proxy'] = ''
    os.environ['https_proxy'] = ''


def get_proxy_settings():
    """
    Get the proxy settings
    :return: dict --  http and https proxy urls
    """
    http_proxy = os.getenv('http_proxy', '')
    https_proxy = os.getenv('https_proxy', '')
    return {
        'http_proxy': http_proxy,
        'https_proxy': https_proxy
    }


def is_valid_url(url):
    """
    Verify if the url is valid
    :param url: url
    :return: bool
    """
    url_regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    if url_regex.match(url):
        return True
    return False


def listify(value):
    """
    Ensure value is a list. If it's not a list, it will be wrapped in one.

    :param value: a value
    :return: list
    """
    if value == '':
        return []
    return value if type(value) == list else [value]


def dictionarize(strings_list, at):
    """
    Convert strings list to a dictionary of key, value pairs by splitting the string on certain 'at' value.

    :param strings_list: list strings to be dictionarized.
    :param split_at: string to split each string at.
    :return: dictionary of splitted strings.
    """
    key_value_pairs = [string.split(at) for string in strings_list]
    filtered_key_value_pairs = filter(lambda pairs_list: len(pairs_list) == 2, key_value_pairs)
    return dict(filtered_key_value_pairs)


def pick_first_element(strings_list):
    """
    Return the first element with text in the list as a list.
    """
    for string in strings_list:
        if len(string.strip()) > 0:
            return [string]
    return [strings_list[0]] if strings_list else []


def is_value_empty(value):
    """
    Returns True if value is logically equivalent to False.

    :param value
    :return: boolean
    """
    if type(value) is list:
        while '' in value:
            value.remove('')
    return not value


def substitute_regex(value, sub_regex='', sub_string=''):
    """
    Return the string obtained by replacing the leftmost
    non-overlapping occurrences of sub_regex in value by the replacement sub_string.
    If the pattern isn't found, value is returned unchanged.
    """
    # Check if there is a regex to be applied on the value before populating it
    return re.sub(sub_regex, sub_string, value.strip())


def rest_get(url, verify_ssl=False, headers=None, data=None):
    """
    Wrapper around python requests to do GET REST calls
    :param url: url to do the REST call to
    :param  verify_ssl:  Ignore certificate and ssl verification if false

    :param headers: REST call header that can contain for example the API token if exist
    :return dict -- Result
    :raises RuntimeError
    """
    # TODO : User agent need to be configurable
    all_headers_data = {'User-Agent': 'KIPP 1.0'}
    username = os.environ.get("YAOOTA_API_USERNAME")
    password = os.environ.get("YAOOTA_API_PASSWORD")
    api_key = os.environ.get("YAOOTA_API_KEY")
    api_token = os.environ.get("YAOOTA_API_TOKEN")
    api_country = os.environ.get("YAOOTA_API_COUNTRY")
    auth = ()
    if not is_valid_url(url):
        raise RuntimeError("URL %s is not valid")
    if headers:
        all_headers_data.update(headers)
    if username and password:
        auth = (username, password)
    if api_key:
        data = {'data': data, 'api_key': api_key}
        data = json.dumps(data, encoding='UTF-8')
    if api_token:
        all_headers_data['App-Token'] = api_token
    if api_country:
        all_headers_data['Country-Code'] = api_country
    get_request = requests.get(url, verify=verify_ssl, auth=auth, headers=all_headers_data, data=data)
    if not get_request.ok:
        details = 'No details are specified'
        try:
            details = get_request.json()['details']
        except Exception, ex:
            details = ex
        finally:
            logging.error('request to %s is not successful: %s', url, details)
        get_request.raise_for_status()
    return get_request.json()


def rest_post(url, verify_ssl=False, headers=None, data=None):
    """
    Wrapper around python requests to do POST REST calls
    :param url: url to do the REST call to
    :param verify_ssl: Ignore certificate and ssl verification if false
    :param username: Authentication user name
    :param password: Authentication password
    :param headers: REST call header that can contain for example the API token if exist
    :param api_key: API key
    :param data : data to be post'd as a python dict
    :return dict -- Result
    :raises RuntimeError
    """
    # TODO : User agent need to be configurable
    all_headers_data = {'User-Agent': 'KIPP 1.0'}
    auth = ()
    username = os.environ.get("YAOOTA_API_USERNAME")
    password = os.environ.get("YAOOTA_API_PASSWORD")
    api_key = os.environ.get("YAOOTA_API_API_KEY")
    api_token = os.environ.get("YAOOTA_API_TOKEN")
    api_country = os.environ.get("YAOOTA_API_COUNTRY")
    if not is_valid_url(url):
        raise RuntimeError("URL %s is not valid")
    if not data:
        logging.warning("Post data for url %s is not provided", url)
    if headers:
        all_headers_data.update(headers)
    if username and password:
        auth = (username, password)
    if api_key:
        data = {'data': data, 'api_key': api_key}
    if api_token:
        all_headers_data['App-Token'] = api_token
    if api_country:
        all_headers_data['Country-Code'] = api_country
    data = json.dumps(data, encoding='UTF-8')
    post_request = requests.post(url, verify=verify_ssl, auth=auth, headers=all_headers_data,
                                 data=data)
    if not post_request.ok:
        details = 'No details are specified'
        try:
            details = post_request.json()['details']
        except Exception, ex:
            details = ex
        finally:
            logging.error('request to %s is not successful: %s', url, details)
        post_request.raise_for_status()
    return post_request.json()


def rest_put(url, verify_ssl=False, headers=None, data=None):
    """
    Wrapper around python requests to do PUT REST calls
    :param url: url to do the REST call to
    :param verify_ssl: Ignore certificate and ssl verification if false
    :param username: Authentication user name
    :param password: Authentication password
    :param headers: REST call header that can contain for example the API token if exist
    :param api_key: API key
    :param data : data to be put as a python dict
    :return dict -- Result
    :raises RuntimeError
    """
    # TODO : User agent need to be configurable
    all_headers_data = {'User-Agent': 'KIPP 1.0'}
    username = os.environ.get("YAOOTA_API_USERNAME")
    password = os.environ.get("YAOOTA_API_PASSWORD")
    api_key = os.environ.get("YAOOTA_API_API_KEY")
    api_token = os.environ.get("YAOOTA_API_TOKEN")
    api_country = os.environ.get("YAOOTA_API_COUNTRY")
    auth = ()
    if headers:
        all_headers_data.update(headers)
    if not is_valid_url(url):
        raise RuntimeError("URL %s is not valid")
    if username and password:
        auth = (username, password)
    if api_key:
        data = {'data': data, 'api_key': api_key}
    if api_token:
        all_headers_data['App-Token'] = api_token
    if api_country:
        all_headers_data['Country-Code'] = api_country
    data = json.dumps(data, encoding='UTF-8')
    response = requests.put(url, verify=verify_ssl, auth=auth, headers=all_headers_data,
                            data=data)
    if not response.ok:
        details = 'No details are specified'
        try:
            details = response.json()['details']
        except Exception, ex:
            details = ex
        finally:
            logging.error('request to %s is not successful: %s', url, details)
        response.raise_for_status()
    return response


def get_next_page_url(url, page_param_name, first_page_num):
    """
    calculates the next page url
    for example:
    if the input is: http://egypt.souq.com/eg-en/baby-accessories/l/
    it must return: http://egypt.souq.com/eg-en/baby-accessories/l/?page=2
    and
    if the input is: http://egypt.souq.com/eg-en/baby-accessories/l/?page=2
    it must return: http://egypt.souq.com/eg-en/baby-accessories/l/?page=3
    :param url:
    :param page_param_name: string representing the page number parameter name in the query string
    :param first_page_num: integer indicating the index of the first page
    :return: url:
    """
    splitted_url = urlsplit(url)
    query_dict = dict(parse_qsl(splitted_url.query))  # get the url query string as a dict
    next_page_number = int(query_dict.get(page_param_name, '%d' % first_page_num)) + 1  # increment page by 1
    query_dict.update({page_param_name: next_page_number})  # update the page number
    next_split_result = SplitResult(splitted_url.scheme, splitted_url.netloc, splitted_url.path, urlencode(query_dict),
                                    splitted_url.fragment)
    return urlunsplit(next_split_result)


def add_min(hour, minute, minutes):
    """
    Adds given number of minutes to a given time in hour and minute
    :param hour:
    :param minute:
    :param minutes: the number of minutes to add
    :return: the given time with the given minutes added
    """
    minute += minutes
    hour += minute / 60
    minute %= 60
    return (hour, minute)


def get_slack_logger():
    """
    Return a slack logger
    :return: object
    """
    web_hook = os.environ.get("SLACK_WEB_HOOK")
    if not web_hook:
        raise RuntimeError("Slack token is not valid")
    settings = get_project_settings()
    channel = settings.get('CHANNEL')
    slack_user = settings.get("SLACK_USER")
    slack_logger = slackpy.SlackLogger(web_hook, channel, slack_user)
    return slack_logger


def propagate_msg_to_slack(logger, title, message, log_level):
    """
    Method that propagate message to slack channel this uses slackpy https://github.com/iktakahiro/slackpy
    :param logger: slack logger object
    :param message: message
    :param log_level: severity level ex. ["DEBUG", "INFO", "WARNING","ERROR"]
    :return: None
    """
    severity_level_map = {
        "DEBUG": ("#03A9F4", 10),
        "INFO": ("good", 20),
        "WARNING": ("warning", 30),
        "ERROR": ("danger", 40)
    }
    if log_level not in severity_level_map:
        raise RuntimeError("Unknown log level")
    logger.message(message=message, title=title, color=severity_level_map[log_level][0],
                   log_level=severity_level_map[log_level][1])


class LogPropagationHandler(logging.Handler):
    """Record log levels count into a crawler stats"""

    def __init__(self, propagation_logger, merchant_name, *args, **kwargs):
        super(LogPropagationHandler, self).__init__(*args, **kwargs)
        self.propagation_logger = propagation_logger
        self.raise_on_levels = ['ERROR']
        self.merchant_name = merchant_name

    def emit(self, record):
        if record.levelname in self.raise_on_levels:
            title = "[%s]: %s" % (self.merchant_name, record.pathname)
            propagate_msg_to_slack(logger=self.propagation_logger, title=title, message=record.message,
                                   log_level=record.levelname)


def get_safe_html(html_text):
    """
    Uses bleach.clean(): https://bleach.readthedocs.org/en/latest/clean.html#
    to remove all the tags and attributes in the given html_text except the ones in allowed_tags and allowed_attrs.
    :param html_text: string representing html
    :return: string representing the cleaned html
    """
    allowed_attrs = {
        'a': ['href', 'rel'],
        'img': ['src', 'alt']
    }
    allowed_tags = ['div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ol', 'ul', 'li', 'strong', 'a', 'img', 'i', 'em',
                    'section', 'small',
                    'main', 'aside', 'article', 'b', 'table', 'thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'span']
    return bleach.clean(text=html_text, tags=allowed_tags, attributes=allowed_attrs, strip=True)


def get_sentry_client():
    """
    Method that create a sentry client and return it
    :return: Object -- sentry client
    """
    sentry_dsn_key = os.environ.get("SENTRY_KEY")
    if sentry_dsn_key:
        return sentry_client(sentry_dsn_key)
    return ''


def propagate_msg_to_sentry(sentry_client, msg):
    """
    Method that propagate(send) message to sentry
    :param sentry_client: object -- sentry client object
    :param msg: string -- message
    :return: None
    """
    if not sentry_client:
        raise IOError("Sentry client is not found")
    sentry_client.captureMessage(msg)


def is_crawl_task_exists(merchant_name, spider_name):
    """
    Check if crawl task with given merchan_name and spider_name is already running
    :param merchant_name: str representing merchant name
    :param spider_name:  str representing spider name
    :return: boolean
    """
    if not merchant_name or not spider_name:
        raise RuntimeError("Merchant and Spider names cannot be None or empty strings")
    merchant_name = 'merchant_name=%s' % merchant_name
    for process in psutil.process_iter():
        cmd_line = process.cmdline()
        if merchant_name in cmd_line and spider_name in cmd_line:
            logging.info("Crawl task with merchant %s and spider %s is already running with pid %s", merchant_name,
                         spider_name, process.pid)
            return True

    return False


def is_merchant_process_exists(merchant_name):
    """
    Check if merchant has a process already running
    :param merchant_name: string -- merchant
    :return: boolean
    """
    if not merchant_name:
        raise RuntimeError("Merchant name cannot be None or empty string")
    process_cmd_line = 'merchant_name=%s' % merchant_name
    for process in psutil.process_iter():
        if process_cmd_line in process.cmdline():
            logging.info("Merchant %s is already running with pid %s", merchant_name, process.pid)
            return True
    return False


def get_1st_run_marker_path(merchant_name):
    """
    construct the 1st run marker path
    :param merchant_name: string -- merchant name
    :return: string -- marker file path
    """
    marker_name = ".already_ran"
    project_settings = get_project_settings()
    log_dir = project_settings.get("LOG_DIR", "/var/kipp/logs")
    marker_file_path = os.path.join(log_dir, merchant_name, marker_name)
    return marker_file_path


def write_merchant_1st_run_marker(merchant_name):
    """
    Write the 1st run marker for merchant - This method is made only for the 1st deployment
    :param merchant_name: string -- merchant name
    :return:
    """
    marker_file_path = get_1st_run_marker_path(merchant_name=merchant_name)
    os.mknod(marker_file_path)


def merchant_1st_run_marker_found(merchant_name):
    """
    Check if the 1st run marker for merchant exist
    :param merchant_name: string -- merchant name
    :return: boolean
    """
    marker_file_path = get_1st_run_marker_path(merchant_name=merchant_name)
    if os.path.exists(marker_file_path):
        return True
    return False


def from_url_to_tag(url):
    url_tag = ET.Element(tag='url')
    loc_tag = ET.SubElement(url_tag, tag='loc')
    loc_tag.text = url
    return url_tag


def write_urls_xml(file_path, urls):
    """
    This method writes urls xml file based on xml schema for the sitemap protocol described here:
    http://www.sitemaps.org/protocol.html
    :param file_path: the file path of the written xml file
    :param urls: iterable of urls to be writen
    :return: none
    """
    XML_NAMESPACE = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    root_tag = ET.Element(tag='urlset', attrib={'xmlns': XML_NAMESPACE})
    for url in urls:
        root_tag.append(from_url_to_tag(url))
    sitemap_tree = ET.ElementTree(element=root_tag)
    sitemap_tree.write(file_path, encoding='UTF-8')


def get_expanded_name(tag):
    """
    :param tag: str representing xml tag name
    :return: expanded tag name with its namespace as defined here:
    https://docs.python.org/2/library/xml.etree.elementtree.html#parsing-xml-with-namespaces
    """
    XML_NAMESPACE = 'http://www.sitemaps.org/schemas/sitemap/0.9'
    return '{%s}%s' % (XML_NAMESPACE, tag)


def read_urls_xml(file_path):
    """
    reads urls xml file the follows the sitemap xml schema described here:
    http://www.sitemaps.org/protocol.html
    :param file_path: the file path on the urls' xml
    :return: generator of the urls contained in the xml file with the given path
    """
    try:
        urls_tree = ET.parse(file_path)
    except IOError:
        return
    else:
        # get its root
        sitemap_tree = urls_tree.getroot()
        url_tag_name = get_expanded_name('url')
        # iterate over all url tags that are direct childs of the root
        for url_tag in sitemap_tree.findall(url_tag_name):
            loc_tag_name = get_expanded_name('loc')
            # get the loc tag that its text will be the product url
            loc_tag = url_tag.find(loc_tag_name)
            yield loc_tag.text


def notify_sentry(messages):
    """
    send the given messages to sentry
    :param messages: list of strings
    :return: None
    """
    if messages:
        sentry_client = get_sentry_client()
        if sentry_client:
            for message in messages:
                propagate_msg_to_sentry(sentry_client=sentry_client, msg=message)
        else:
            logging.info("Sentry is not configured.. Skipping")
