from scrapely import Scraper
from twisted.web.resource import Resource
from .resource import SlydJsonResource
from scrapy import log
import errno
import os
import json

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
        params = self.read_json(request)
        templates, spider_name = self._create_spider(
            request.project, request.auth_info, params)
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
        template_names = [template['name'] for template in templates]
        return template_names

    def _create_spider(self, project, auth_info, params, **kwargs):
        spider = params.get('spider')
        if spider is None:
            return None, None
        pspec = self.scrapelyd.spec_manager.project_spec(project, auth_info)
        try:
            spider_spec = pspec.spider_with_templates(spider)
            return (spider_spec['templates'], spider)
        except IOError as ex:
            if ex.errno == errno.ENOENT:
                log.msg("skipping extraction, no spec: %s" % ex.filename)
                return None
            else:
                raise
