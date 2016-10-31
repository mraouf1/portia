"""
kipp custom image pipeline
"""
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FSFilesStore
from scrapy.exceptions import DropItem
import hashlib
import os
import os.path
import rfc822
import logging
import six

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

from twisted.internet import defer, threads
from scrapy.http import Request

from slybot.kipp_utils import utils
from slybot.kipp_utils.utils import timing

logger = logging.getLogger(__name__)


# s3 class
class S3FilesStore(object):
    """
    S3 file store
    """
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
    AWS_HOST = None

    POLICY = 'public-read'
    HEADERS = {
        'Cache-Control': 'max-age=172800',
    }

    def __init__(self, uri):
        assert uri.startswith('s3://')
        self.bucket, self.prefix = uri[5:].split('/', 1)
        self.http_proxy = ''
        self.https_proxy = ''

    @timing("KippImagePipeline", logging)
    def stat_file(self, path, info):
        def _onsuccess(boto_key):
            checksum = boto_key.etag.strip('"')
            last_modified = boto_key.last_modified
            modified_tuple = rfc822.parsedate_tz(last_modified)
            modified_stamp = int(rfc822.mktime_tz(modified_tuple))
            return {'checksum': checksum, 'last_modified': modified_stamp}

        return self._get_boto_key(path).addCallback(_onsuccess)

    def _get_boto_bucket(self):
        # Disable kipp proxy to be able to connect to S3
        # 1st get the proxy configuration
        if not (self.http_proxy or self.https_proxy):
            proxy_config = utils.get_proxy_settings()
            self.http_proxy = proxy_config['http_proxy']
            self.https_proxy = proxy_config['https_proxy']
        utils.disable_proxy()
        from boto.s3.connection import S3Connection

        if self.AWS_HOST:
            # We are using AWS_HOST here to workaround the encryption scheme for the buckets created in europe
            c = S3Connection(self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY, is_secure=False,
                             host=self.AWS_HOST)
        else:
            c = S3Connection(self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY, is_secure=False)
        return c.get_bucket(self.bucket, validate=False)

    def _get_boto_key(self, path):
        b = self._get_boto_bucket()
        key_name = '%s%s' % (self.prefix, path)
        return threads.deferToThread(b.get_key, key_name)

    @timing("KippImagePipeline", logging)
    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to S3 storage"""
        logging.info("Uploading image to AWS from path %s", path)
        b = self._get_boto_bucket()
        key_name = '%s%s' % (self.prefix, path)
        k = b.new_key(key_name)
        if meta:
            for metakey, metavalue in six.iteritems(meta):
                k.set_metadata(metakey, str(metavalue))
        h = self.HEADERS.copy()
        if headers:
            h.update(headers)
        buf.seek(0)

        utils.enable_proxy(http_proxy=self.http_proxy, https_proxy=self.https_proxy)

        return threads.deferToThread(k.set_contents_from_string, buf.getvalue(),
                                     headers=h, policy=self.POLICY)


# Kipp images pipeline class
class KippImagePipeline(ImagesPipeline):
    """
    Kipp Image pipeline
    Features:
        - Local storage
        - s3 storage
        - Amazon EU hosts support
    """
    MEDIA_NAME = "file"
    EXPIRES = 90
    STORE_SCHEMES = {
        '': FSFilesStore,
        'file': FSFilesStore,
        's3': S3FilesStore,
    }
    MEDIA_NAME = 'image'
    MIN_WIDTH = 0
    MIN_HEIGHT = 0
    THUMBS = {}
    DEFAULT_IMAGES_URLS_FIELD = 'image_urls'
    DEFAULT_IMAGES_RESULT_FIELD = 'images'

    @classmethod
    def from_settings(cls, settings):
        cls.MIN_WIDTH = settings.getint('IMAGES_MIN_WIDTH', 0)
        cls.MIN_HEIGHT = settings.getint('IMAGES_MIN_HEIGHT', 0)
        cls.EXPIRES = settings.getint('IMAGES_EXPIRES', 90)
        cls.THUMBS = settings.get('IMAGES_THUMBS', {})
        s3store = cls.STORE_SCHEMES['s3']
        # Making sure that s3 keys are None ( not empty string) if they are not presented
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        s3store.AWS_ACCESS_KEY_ID = aws_access_key if aws_access_key else None
        s3store.AWS_SECRET_ACCESS_KEY = aws_secret_key if aws_access_key else None
        s3store.AWS_HOST = settings['AWS_HOST']

        cls.IMAGES_URLS_FIELD = settings.get('IMAGES_URLS_FIELD', cls.DEFAULT_IMAGES_URLS_FIELD)
        cls.IMAGES_RESULT_FIELD = settings.get('IMAGES_RESULT_FIELD',
                                               cls.DEFAULT_IMAGES_RESULT_FIELD)
        cls.IMAGES_DIR = settings.get('IMAGES_DIR', 'images')
        cls.IMAGES_SUB_DIR = settings.get('IMAGES_SUB_DIR', 'images')
        store_uri = settings['IMAGES_STORE']
        return cls(store_uri)


    def file_path(self, request, response=None, info=None):
        """
        File path method which return the path of the file that will be stored
        :param request: http request object
        :param response: http response object
        :param info: file info
        :return: path of the file that will be stored
        """
        # start of deprecation warning block (can be removed in the future)
        def _warn():
            from scrapy.exceptions import ScrapyDeprecationWarning
            import warnings

            warnings.warn('ImagesPipeline.image_key(url) and file_key(url) methods are deprecated, '
                          'please use file_path(request, response=None, info=None) instead',
                          category=ScrapyDeprecationWarning, stacklevel=1)

        # check if called from image_key or file_key with url as first argument
        if not isinstance(request, Request):
            _warn()
            url = request
        else:
            url = request.url

        # detect if file_key() or image_key() methods have been overridden
        if not hasattr(self.file_key, '_base'):
            _warn()
            return self.file_key(url)
        elif not hasattr(self.image_key, '_base'):
            _warn()
            return self.image_key(url)
        # end of deprecation warning block
        image_guid = hashlib.sha1(url).hexdigest()  # change to request.url after deprecation
        return '%s/%s.jpg' % (self.IMAGES_DIR, image_guid)

    @timing("KippImagePipeline", logging)
    def item_completed(self, results, item, info):
        """
        Drops the scraped item if it has no successfully downloaded images,
        if it has at least one successfully downloaded image, update item's main_image and extra_image according to
        the successfully downloaded images and return it
        :param results: list of 2-elements tuples representing the downloading results,
                        see: http://doc.scrapy.org/en/latest/topics/media-pipeline.html#module-scrapy.pipelines.files
                        for more information
        :param item: scrapped item
        :param info:
        :return: the scraped item if it hasn't been dropped
        """
        super(KippImagePipeline, self).item_completed(results, item, info)
        # get all the downloaded images' urls
        imgs_urls = [img['url'] for img in item['images']]
        # if there are no successfully downloaded images
        if not imgs_urls:
            raise DropItem('Dropping Item at %s, all of its images failed to be downloaded' % item['url_en'])
        # if the main image hasn't been downloaded successfully
        if item['main_image'] not in imgs_urls:
            # get the last the extra_image and set it as the main one
            item['main_image'] = imgs_urls.pop()
        else:
            imgs_urls.remove(item['main_image'])
        item['extra_images'] = imgs_urls
        return item
