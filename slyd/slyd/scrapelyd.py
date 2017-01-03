from scrapely import Scraper
from twisted.web.resource import Resource
from .resource import SlydJsonResource
from scrapy import log
import errno
import os
import json

MERCHANT_SETTING_BASE = """
# Automatically created by: slyd
# -*- coding: utf-8 -*-

from scrapy.contrib.spiders import Rule
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor

LOG_FILE = '/var/kipp/logs/{merchant_name}.log'
COUNTRY_CODE = "{country_code}"
CURRENCY_CODE = "{currency_code}"
USE_SCRAPELY = True
START_URLS = {start_urls}
ALLOWED_DOMAINS = {allowed_domains}
MERCHANT_URLS_CONFIG = [{{"url": "{merchant_url}", 'cookie_config': {general_cookie} }}]
RULES = [Rule(LxmlLinkExtractor(allow={allow_regex},
                                deny={deny_regex}),
              callback='parse_item', follow=True)]
localization_config = {{
    'english': {{
        'url': "{english_url}",
        'cookie_config': {english_language_cookie},
        'url_args': {english_url_args}
    }},
    'arabic': {{
        'url': "{arabic_url}",
        'cookie_config': {arabic_language_cookie},
        'url_args': {arabic_url_args}
    }}
}}
"""

SCRAPELY_TEMPLATES_DIR = '/var/kipp/scrapely_templates'
KIPP_MERCHANT_SETTINGS_DIR = '/apps/kipp/kipp/kipp_base/kipp_settings/{country_code}'
if not os.path.exists(SCRAPELY_TEMPLATES_DIR):
    os.makedirs(SCRAPELY_TEMPLATES_DIR)


def create_scrapelyd_resource(spec_manager):
    scrapelyd = SlydScrapely(spec_manager.settings, spec_manager)
    scrapelyd.putChild('train', Train(scrapelyd))
    return scrapelyd


class SlydScrapely(Resource):

    def __init__(self, settings, spec_manager):
        Resource.__init__(self)
        self.scraper = Scraper()
        self.spec_manager = spec_manager
        log.msg("scrapely initialized", level=log.DEBUG)


class ScrapelyResource(SlydJsonResource):
    def __init__(self, scrapelyd):
        Resource.__init__(self)
        self.scrapelyd = scrapelyd


class Train(ScrapelyResource):
    isLeaf = True

    def render_POST(self, request):
        """
        define endpoint for scrapely/train API. Create the spider and generate scrapely template
        :param request:
        :return: template names used in training scrapely
        """
        params = self.read_json(request)
        spider_name = params.get('spider')
        spider_spec = self._create_spider(request.project, request.auth_info, spider_name)
        self._create_kipp_setting(spider_name, spider_spec)
        templates = spider_spec['templates']
        template_names = self._get_templates_name(templates)
        log.msg('Start generating scrapely templates for %s Spider' % params.get('spider'))
        scrapely_templates = self._generate_scrapely_templates(templates)
        log.msg('Scrapely templates generated from templates %s' % str(template_names))

        scrapely_file_name = "%s.json" % spider_name
        scrapely_file_path = os.path.join(SCRAPELY_TEMPLATES_DIR, scrapely_file_name)
        if os.path.exists(scrapely_file_path):
            os.remove(scrapely_file_path)

        with open(scrapely_file_path, "w") as outfile:
            json.dump({"templates": scrapely_templates}, outfile)

        log.msg('Scraper instance is saved at %s' % SCRAPELY_TEMPLATES_DIR)
        return json.dumps({"template_names": template_names})

    def _generate_scrapely_templates(self, templates):
        """
        Combine all templates in a list and add headers
        :param templates:
        :return: scrapely_templates: a list of all templates for this spider
        """
        scrapely_templates = []
        for template in templates:
            scrapely_template = dict()
            scrapely_template['url'] = template['url']
            scrapely_template['headers'] = template.get('headers', {})
            scrapely_template['encoding'] = template.get('encoding', 'utf-8')
            scrapely_template['body'] = template.get('annotated_body', '')
            scrapely_template['page_id'] = template.get('page_id', '')
            scrapely_templates.append(scrapely_template.copy())
        return scrapely_templates

    def _get_templates_name(self, templates):
        """
        get the templates and return the template names
        :param templates:
        :return: template_names: template names of templates
        """
        template_names = [template['name'] for template in templates]
        return template_names

    def _create_spider(self, project, auth_info, spider_name, **kwargs):
        """
        create the spider from the spec manager
        :param project:
        :param auth_info:
        :param spider_name:
        :param kwargs:
        :return:
        """
        if spider_name is None:
            return None
        pspec = self.scrapelyd.spec_manager.project_spec(project, auth_info)
        try:
            spider_spec = pspec.spider_with_templates(spider_name)
            return spider_spec
        except IOError as ex:
            if ex.errno == errno.ENOENT:
                log.msg("skipping extraction, no spec: %s" % ex.filename)
                return None
            else:
                raise

    def _create_kipp_setting(self, merchant_name, spider_spec):
        """
        preprocess spider specs and call _create_setting_file function
        :param merchant_name:
        :param country:
        :param spider_spec:
        :return:
        """
        country_code = spider_spec['country_code']
        kipp_country_setting_dir = KIPP_MERCHANT_SETTINGS_DIR.format(country_code=country_code)
        if not os.path.exists(kipp_country_setting_dir):
          os.makedirs(kipp_country_setting_dir)
        merchant_file_path = kipp_country_setting_dir + '/' + merchant_name + '.py'
        country_code = spider_spec['country_code']
        currency_code = spider_spec['currency_code']
        start_urls = spider_spec['start_urls']
        merchant_url = spider_spec['start_urls'][0]
        allow_regex = spider_spec['follow_patterns']
        allowed_domains = start_urls[0].split("//")[-1].split("/")[0].replace("www.","")
        allowed_domains = [allowed_domains]
        deny_regex = spider_spec['exclude_patterns']
        english_url = spider_spec['english_url']
        arabic_url = spider_spec['arabic_url']
        english_url_args = spider_spec['english_url_args']
        arabic_url_args = spider_spec['arabic_url_args']
        english_cookie_name = spider_spec['english_cookie_name']
        english_cookie_value = spider_spec['english_cookie_value']
        arabic_cookie_name = spider_spec['arabic_cookie_name']
        arabic_cookie_value = spider_spec['arabic_cookie_value']
        use_language_cookies = spider_spec['cookies_enabled']
        use_currency_cookies = spider_spec['use_currency_cookies']
        currency_cookie_name = spider_spec['currency_cookie_name']
        currency_cookie_value = spider_spec['currency_cookie_value']
        self._create_setting_file(merchant_file_path, merchant_name=merchant_name, country_code=country_code,
                                  start_urls=start_urls, allowed_domains=allowed_domains, merchant_url=merchant_url,
                                  allow_regex=allow_regex, deny_regex=deny_regex, currency_code=currency_code,
                                  english_url=english_url, arabic_url=arabic_url, english_url_args=english_url_args,
                                  arabic_url_args= arabic_url_args, english_cookie_name=english_cookie_name,
                                  english_cookie_value=english_cookie_value, arabic_cookie_name=arabic_cookie_name,
                                  arabic_cookie_value=arabic_cookie_value, use_language_cookies=use_language_cookies,
                                  use_currency_cookies=use_currency_cookies, currency_cookie_name=currency_cookie_name,
                                  currency_cookie_value=currency_cookie_value)

    def _create_setting_file(self, file_path, **kwargs):
        """
        create setting file on the disk
        :param file_path:
        :param args:
        :return:
        """
        if kwargs['english_url_args'] :
            english_url_args = "\""+kwargs["english_url_args"] +"\""
            kwargs['english_url_args'] = english_url_args
        else:
            kwargs['english_url_args'] = None
        if kwargs['arabic_url_args']:
            arabic_url_args =  "\""+kwargs["arabic_url_args"] +"\""
            kwargs['arabic_url_args'] =  arabic_url_args
        else:
          kwargs['arabic_url_args'] = None

        if kwargs['use_language_cookies']:
            english_language_cookie = """
                {{'name':"{english_cookie_name}", 'value': "{english_cookie_value}",
                'domain': ".{allowed_domains[0]}", 'path': '/'}}
                """.format(**kwargs)
            arabic_language_cookie = """
                {{'name': "{arabic_cookie_name}", 'value': "{arabic_cookie_value}",
                'domain': '.{allowed_domains[0]}', 'path': '/'}}
                """.format(**kwargs)
        else:
            english_language_cookie = None
            arabic_language_cookie = None
        if kwargs['use_currency_cookies']:
            currency_cookie = """
                {{'name':"{currency_cookie_name}", 'value': "{currency_cookie_value}",
                'domain': ".{allowed_domains[0]}", 'path': '/'}}
                """.format(**kwargs)
        else:
            currency_cookie = None

        if kwargs['use_language_cookies'] and kwargs['use_currency_cookies']:
            general_cookie = """
                [{}, {}]
                """.format(english_language_cookie,currency_cookie)
        elif kwargs['use_language_cookies']:
            general_cookie = """
                [{}]
                """.format(english_language_cookie)
            if english_language_cookie:
                english_language_cookie = "["+english_language_cookie+"]"
            if arabic_language_cookie:
                arabic_language_cookie = "["+arabic_language_cookie+"]"
        elif kwargs['use_currency_cookies']:
            general_cookie = """
                [{currency_cookie}]
                """.format(currency_cookie)
            currency_cookie = [currency_cookie]
        else:
            general_cookie = None

        kwargs.setdefault('general_cookie', general_cookie)
        kwargs.setdefault('english_language_cookie', english_language_cookie)
        kwargs.setdefault('arabic_language_cookie', arabic_language_cookie)
        kwargs.setdefault('currency_cookie', currency_cookie)

        merchant_setting = MERCHANT_SETTING_BASE.format(**kwargs)

        with open(file_path, 'w') as f:
            f.write(merchant_setting)
