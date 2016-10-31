# coding:utf-8
"""
scrapy-mongodb - MongoDB pipeline for Scrapy

Homepage: https://github.com/sebdah/scrapy-mongodb
Author: Sebastian Dahlgren <sebastian.dahlgren@gmail.com>
License: Apache License 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>

Copyright 2013 Sebastian Dahlgren

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import datetime
import logging
import copy
import os
import urllib

from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from scrapy.exporters import BaseItemExporter
from slybot.kipp_utils.utils import timing
VERSION = '0.9.0'


def not_set(string):
    """ Check if a string is None or ''

    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(BaseItemExporter):
    """ MongoDB pipeline class """
    # Default options
    config = {
        'uri': 'mongodb://192.168.33.20:27017',
        'fsync': False,
        'write_concern': 0,
        'port': 27017,
        'database': 'kipp_analytics',
        'replica_set_name': '',
        'collection': 'items',
        'replica_set': None,
        'unique_key': None,
        'buffer': None,
        'append_timestamp': False,
        'stop_on_duplicate': 0,
    }

    # Item buffer
    current_item = 0
    item_buffer = []
    crawler = None
    settings = None
    collection = None

    # Duplicate key occurrence  count
    duplicate_key_count = 0

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

    def open_spider(self, spider):
        self.load_spider(spider)

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                port=self.config['port'],
                fsync=self.config['fsync'],
                replicaSet=self.config['replica_set_name'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection
        database = connection[self.config['database']]
        self.collection = database[self.config['collection']]
        logging.info('Connected to MongoDB %s, using "%s/%s"',
                     self.config['uri'],
                     self.config['database'],
                     self.config['collection'])

        # Ensure unique index
        if self.config['unique_key']:
            self.collection.ensure_index(self.config['unique_key'], unique=True)
            logging.info('uEnsuring index for key %s', self.config['unique_key'])

        # Get the duplicate on key option
        if self.config['stop_on_duplicate']:
            tmp_value = self.config['stop_on_duplicate']
            if tmp_value < 0:
                logging.error(u'Negative values are not allowed for MONGODB_STOP_ON_DUPLICATE option.')
                raise SyntaxError(
                    (
                        'Negative values are not allowed for'
                        ' MONGODB_STOP_ON_DUPLICATE option.'
                    )
                )
            self.stop_on_duplicate = self.config['stop_on_duplicate']
        else:
            self.stop_on_duplicate = 0

    @timing("MongoDBPipeline", logging)
    def configure(self):
        """ Configure the MongoDB connection """
        # Set all regular options
        # TODO: Remove the usage of MONGODB_URI config
        options = [
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('port', 'MONGODB_PORT'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
            ('stop_on_duplicate', 'MONGODB_STOP_ON_DUPLICATE')
        ]
        # Getting the URI
        mongo_user = os.environ.get("MONGODB_USER")
        mongo_password = os.environ.get("MONGODB_PASS")
        mongo_password = urllib.quote_plus(mongo_password)
        mongo_ip = self.settings['MONGODB_IP']
        database = self.settings['MONGODB_DATABASE']
        self.config['replica_set_name'] = os.environ.get("REPLICA_SET_NAME")
        mongodb_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
        if mongodb_connection_string:
            uri = mongodb_connection_string.replace("YOOTA_MONGODB_IP", mongo_ip)
        else:
            uri = "mongodb://%s:%s@%s/%s" % (mongo_user, mongo_password, mongo_ip, database)

        self.config['uri'] = uri

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            logging.error(u'IllegalConfig: Settings both MONGODB_BUFFER_DATA and MONGODB_UNIQUE_KEY is not supported')
            raise SyntaxError(
                (
                    u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                    u'and MONGODB_UNIQUE_KEY is not supported'
                ))

    @timing("MongoDBPipeline", logging)
    def process_item(self, item, spider):
        """ Process the item and add it to MongoDB

        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        mongo_item = copy.deepcopy(item)
        mongo_item = dict(self._get_serialized_fields(mongo_item))
        mongo_item['merchant_name'] = spider.merchant_name

        if self.config['buffer']:
            self.current_item += 1

            if self.config['append_timestamp']:
                mongo_item['kipp_analytics'] = {'ts': datetime.datetime.utcnow()}

            self.item_buffer.append(mongo_item)

            if self.current_item == self.config['buffer']:
                self.current_item = 0
                return self.insert_item(self.item_buffer, spider)

            else:
                return mongo_item

        self.insert_item(mongo_item, spider)
        return item

    def close_spider(self, spider):
        """ Method called when the spider is closed

        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        if self.item_buffer:
            self.insert_item(self.item_buffer, spider)

    @timing("MongoDBPipeline", logging)
    def insert_item(self, item, spider):
        """ Process the item and add it to MongoDB

        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns:
        """
        if not isinstance(item, list):
            item = dict(item)

            if self.config['append_timestamp']:
                item['kipp_analytics'] = {'ts': datetime.datetime.utcnow()}

        # self.collection.insert(item, continue_on_error=True)
        logging.debug(u'Stored item(s) in MongoDB %s/%s', self.config['database'], self.config['collection'])
