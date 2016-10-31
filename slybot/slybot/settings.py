from __future__ import absolute_import
SPIDER_MANAGER_CLASS = 'slybot.spidermanager.SlybotSpiderManager'
EXTENSIONS = {'slybot.closespider.SlybotCloseSpider': 1}
ITEM_PIPELINES = {'slybot.dupefilter.DupeFilterPipeline': 1,
                  # 'slybot.kipp_pipelines.duplicates_filter.DuplicatesPipeline': 2,
                  # 'slybot.kipp_pipelines.mongodb.MongoDBPipeline': 3,
                  # 'slybot.kipp_pipelines.update_crawl_iteration.UpdateCrawlIteration': 4,
                  # 'slybot.kipp_pipelines.item_validation.ItemValidationPipeline': 5,
                  # 'slybot.kipp_pipelines.images.KippImagePipeline': 6,
                  # 'slybot.kipp_pipelines.yaoota_db.YaootaDBPipeline': 7
                  }
SPIDER_MIDDLEWARES = {'slybot.spiderlets.SpiderletsMiddleware': 999}  # as close as possible to spider output
DOWNLOADER_MIDDLEWARES = {
    'slybot.pageactions.PageActionsMiddleware': 700,
    'slybot.splash.SlybotJsMiddleware': 725
}
PLUGINS = [
    'slybot.plugins.scrapely_annotations.Annotations',
    'slybot.plugins.selectors.Selectors'
]

SLYDUPEFILTER_ENABLED = True
DUPEFILTER_CLASS = 'scrapyjs.SplashAwareDupeFilter'
PROJECT_DIR = 'slybot-project'
FEED_EXPORTERS = {
    'csv': 'slybot.exporter.SlybotCSVItemExporter',
}
CSV_EXPORT_FIELDS = None
# KIPP
import os
USER_AGENT_LIST = "/var/kipp/conf/random_agents.kipp"
KIPP_PROXY = 'http://localhost:8123'
# Database API url - merchant id or name must be filled
YAOOTA_API_URL = os.environ.get("YAOOTA_API_URL")
# Database API call headers and here we can put the token for example
YAOOTA_API_HEADERS = {'Content-Type': 'application/json'}
# Database url ssl verification
YAOOTA_API_SSL_VERIFICATION = False
# Log dir
LOG_DIR = '/var/kipp/logs'

# s3 configuration
#IMAGES_STORE = "s3://<bucket_name>/"
#AWS_HOST = 's3.eu-central-1.amazonaws.com'
#IMAGES_STORE = "s3://yaootaweb-bleed/"
# local storage configuration
IMAGES_STORE = '/var/kipp/images'
# Scrapyd URL
SCRAPYD_URL = 'http://localhost:6800'
# Feeds dir
FEEDS_DIR = '/var/kipp/feeds'
# directory for JOBDIR setting
JOBS_DIRECTORY = '/var/kipp/jobsdir'
# Added expiry for kipp images
IMAGES_EXPIRES = 2
# Image directory
IMAGES_DIR = 'media/crawledproductimages'
# Image subdir at which images are stored
IMAGES_SUB_DIR = 'crawledproductimages'
# Mongodb configuration
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE")
MONGODB_COLLECTION = os.environ.get("MONGODB_COLLECTION")
MONGODB_ADD_TIMESTAMP = True
MONGODB_IP = os.environ.get("MONGODB_IP")
MONGODB_PORT = os.environ.get("MONGODB_PORT")
MONGODB_REPLICA = True

try:
    from .local_slybot_settings import *
except ImportError:
    pass
