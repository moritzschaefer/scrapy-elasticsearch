# -*- coding: utf-8 -*-
# Copyright 2014 Michael Malocha <michael@knockrentals.com> and 2015 Moritz Schäfer
#
# Expanded from the work by Julien Duponchelle <julien@duponchelle.info>.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Elastic Search Pipeline for scrappy expanded  with support for multiple items"""

from pyes import ES
from scrapy import log
import hashlib
import types
from datetime import datetime

class ElasticSearchPipeline(object):
    settings = None
    es = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        ext.settings = crawler.settings

        basic_auth = {}

        if (ext.settings['ELASTICSEARCH_USERNAME']):
            basic_auth['username'] = ext.settings['ELASTICSEARCH_USERNAME']

        if (ext.settings['ELASTICSEARCH_PASSWORD']):
            basic_auth['password'] = ext.settings['ELASTICSEARCH_PASSWORD']

        if ext.settings['ELASTICSEARCH_PORT']:
            uri = "%s:%d" % (ext.settings['ELASTICSEARCH_SERVER'], ext.settings['ELASTICSEARCH_PORT'])
        else:
            uri = "%s" % (ext.settings['ELASTICSEARCH_SERVER'])

        ext.es = ES([uri], basic_auth=basic_auth)
        return ext

    # move to helper.py
    # TODO: define mapping not analyzing some stuff
    # TODO: generate categories
    # TODO: add country and competitor!

    def _generate_upsert(self, item):
        upsert_item = dict((key, item[key]) for key in item.fields if key not in ['price', 'old_price', 'size', 'sales_notes', 'sku', 'barcode']),
        upsert_item['prices'] = [{'price': item['price'], 'old_price': item['old_price'], 'date': datetime.now()}]
        upsert_item['categories'] = []
        upsert_item['sizes'] = [dict((key, item[key]) for key in ['size', 'sales_notes', 'sku', 'barcode'])]


    def _generate_update(self, item):
        if item.get('quantity'):
            update_item = {}
            params = {'quantity': item['quantity'], 'size': item['size']}
            script = '''
                index = 0;
                foreach(saved_size : ctx._source.sizes) {
                    if(saved_size.size == size) {
                        ctx._source.sizes[index].quantity = quantity;
                    }
                    index += 1;
                }
            '''
        else:
            update_item = dict((key, item[key]) for key in item.fields if key not in ['price', 'old_price', 'size', 'sales_notes', 'sku', 'barcode']),
            params = {'price': {'price': item['price'], 'old_price': item['old_price'], 'date': datetime.now()},
                      'size': dict((key, item[key]) for key in ['size', 'sales_notes', 'sku', 'barcode'])}
            # add price and add size if size not already exists
            script = '''
                ctx._source.prices += price;
                foreach(saved_size : ctx._source.sizes) {
                    if(saved_size.size == size.size) {
                        return;
                    }
                }
                ctx._source.size += size;
            '''

        return update_item, params, script

    def index_item(self, item):
        if self.settings.get('ELASTICSEARCH_UNIQ_KEY'):
            uniq_key = self.settings.get('ELASTICSEARCH_UNIQ_KEY')
            local_id = hashlib.sha1(item[uniq_key]).hexdigest()
            log.msg("Generated unique key %s" % local_id, level=self.settings.get('ELASTICSEARCH_LOG_LEVEL'))
        else:
            local_id = item['id']

        upsert_doc = self._generate_upsert(item)
        update_doc, params, script = self._generate_upsert(item)


        return_status = self.es.update(dict(item),
                index=self.settings.get('ELASTICSEARCH_INDEX'),
                doc_type=self.settings.get('ELASTICSEARCH_TYPE'),
                lang='mvel',
                script=script,
                params=params,
                id=local_id,
                document=update_doc,
                upsert=upsert_doc)

        if return_status['created']:
            # TODO get categories!
            pass



    def process_item(self, item, spider):
        import ipdb; ipdb.set_trace()
        if isinstance(item, types.GeneratorType) or isinstance(item, types.ListType):
            for each in item:
                    self.process_item(each, spider)
        else:
            try:
                self.index_item(item)
            except KeyError, e:
                log.msg("Error sending item to elasticsearch %s\n%s\nMost probably the item doesn't exist." %
                        (self.settings.get('ELASTICSEARCH_INDEX'), str(e)),
                        level=self.settings.get('ELASTICSEARCH_LOG_LEVEL'), spider=spider)
            else:
                log.msg("Item sent to Elastic Search %s" %
                        (self.settings.get('ELASTICSEARCH_INDEX')),
                        level=self.settings.get('ELASTICSEARCH_LOG_LEVEL'), spider=spider)
            return item
