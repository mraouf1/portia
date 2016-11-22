from scrapely import Scraper
from twisted.web.resource import Resource
from .resource import SlydJsonResource
from scrapy import log
import errno


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
        templates = self._create_spider(
            request.project, request.auth_info, params)
        template_names = self._get_templates_name(templates)
        log.msg('Start training scrapely for %s Spider' % params.get('spider'))
        data_set = self._get_templates_data_set(templates)
        self.scrapelyd.scraper = self._train_scrapely(self.scrapelyd.scraper, data_set)
        log.msg('Scrapely is trained with templates %s' % str(template_names))
        self.scrapelyd.scraper.tofile('/var/kipp/scrapely_templates')
        log.msg('Scraper instance is saved at /var/kipp/scrapely_template')
        return str(template_names)

    def _train_scrapely(self, scraper, data_set):
        for scrapely_data in data_set:
            scraper.train(scrapely_data['url'], scrapely_data['data'])
        return scraper

    def _get_templates_data_set(self, templates):
        template_data_set = [template['scrapely_data'] for template in templates]
        return template_data_set

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
            return spider_spec['templates']
        except IOError as ex:
            if ex.errno == errno.ENOENT:
                log.msg("skipping extraction, no spec: %s" % ex.filename)
                return None
            else:
                raise
