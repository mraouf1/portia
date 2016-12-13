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
LOG_FILE = '/var/kipp/logs/{merchant_name}.log'
COUNTRY_CODE = "{country_code}"
CURRENCY_CODE = "{currency_code}"
USE_SCRAPELY = True
START_URLS = {start_urls}
ALLOWED_DOMAINS = {allowed_domains}
MERCHANT_URLS_CONFIG = [{{"url": "{merchant_url}", 'cookie_config': None}}]
RULES = [Rule(LxmlLinkExtractor(allow={allow_regex},
                                deny={deny_regex}),
              callback='parse_item', follow=True)]
"""

SCRAPELY_TEMPLATES_DIR = '/var/kipp/scrapely_templates'
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
        KIPP_MERCHANT_SETTINGS_DIR = '/apps/kipp/kipp/kipp_base/kipp_settings/%s' % country_code
        if not os.path.exists(KIPP_MERCHANT_SETTINGS_DIR):
          os.makedirs(KIPP_MERCHANT_SETTINGS_DIR)
        MERCHANT_FILE_PATH = KIPP_MERCHANT_SETTINGS_DIR + '/' + merchant_name + '.py'
        country_code = spider_spec['country_code']
        currency_code = spider_spec['currency_code']
        start_urls = spider_spec['start_urls']
        merchant_url = spider_spec['start_urls'][0]
        allow_regex = spider_spec['follow_patterns']
        allowed_domains = start_urls[0].split("//")[-1].split("/")[0].replace("www.","")
        allowed_domains = [allowed_domains]
        deny_regex = spider_spec['exclude_patterns']
        self._create_setting_file(MERCHANT_FILE_PATH, merchant_name=merchant_name, country_code=country_code,
                                  start_urls=start_urls, allowed_domains=allowed_domains, merchant_url=merchant_url,
                                  allow_regex=allow_regex, deny_regex=deny_regex, currency_code=currency_code)

    def _create_setting_file(self, file_path, **kwargs):
        """
        create setting file on the disk
        :param file_path:
        :param args:
        :return:
        """
        merchant_setting = MERCHANT_SETTING_BASE.format(**kwargs)
        with open(file_path, 'w') as f:
            f.write(merchant_setting)
