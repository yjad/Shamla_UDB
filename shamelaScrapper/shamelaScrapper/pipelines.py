# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import apsw
import scrapy
from scrapy.pipelines.images import ImagesPipeline

TABLE_SHAMELA_OFFICIAL = "books_shamela_official"
TABLE_SHAMELA_REP = "books_shamela_rep"


class SQLiteInsertPipeline(object):
    collection_name = 'scrapy_items'

    def __init__(self, db_file):
        self.db_file = db_file

    @classmethod
    def from_crawler(cls, crawler):
        return cls(db_file=crawler.settings.get('SQLITE_PATH', 'data.sqlite'))

    def open_spider(self, spider):
        self.connection = apsw.Connection(self.db_file)
        cursor = self.connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS ' + TABLE_SHAMELA_OFFICIAL +
                       '(id INTEGER NOT NULL PRIMARY KEY,'
                       'view_count INTEGER,'
                       'date_added TEXT,'
                       'tags TEXT,'
                       'rar_link TEXT,'
                       'pdf_link TEXT,'
                       'online_link TEXT,'
                       'epub_link TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS ' + TABLE_SHAMELA_REP +
                       '(id INTEGER NOT NULL PRIMARY KEY,'
                       'view_count INTEGER,'
                       'date_added TEXT,'
                       'rar_link TEXT,'
                       'uploading_user TEXT)')
        cursor.execute('CREATE TABLE IF NOT EXISTS `pdf_links` '
                       '( `repository` TEXT,`link_text` TEXT, `link_value` TEXT, `book_id` INTEGER NOT NULL ,'
                       'UNIQUE (book_id, link_text,link_value))')

    def close_spider(self, spider):
        # self.cursor.close()
        # self.connection.close(True)
        cursor = self.connection.cursor()
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS `official_date_added` ON `books_shamela_official` (`date_added` DESC)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS `repo_date_added` ON `books_shamela_rep` (`date_added` DESC)")
        pass

    def process_item(self, item, spider):
        cursor = self.connection.cursor()
        if 'rar_link' in item:
            if item['repository'] == 'shamela.ws/index.php':
                cursor.execute("INSERT OR REPLACE INTO " + TABLE_SHAMELA_OFFICIAL + " values("
                                                                                    ":id,"
                                                                                    ":view_count,"
                                                                                    ":date_added,"
                                                                                    ":tags,"
                                                                                    ":rar_link,"
                                                                                    ":pdf_link,"
                                                                                    ":online_link,"
                                                                                    ":epub_link)"
                               , dict(item))
            elif item['repository'] == 'shamela.ws/rep.php':
                cursor.execute("INSERT OR REPLACE INTO " + TABLE_SHAMELA_REP + " values("
                                                                               ":id,"
                                                                               ":view_count,"
                                                                               ":date_added,"
                                                                               ":rar_link,"
                                                                               ":uploading_user)"
                               , dict(item))
        else:
            if item['repository'] == 'shamela.ws/index.php':
                cursor.execute('Update books_shamela_official set view_count =? where id=?',
                               (item['view_count'], item['id']))
            elif item['repository'] == 'shamela.ws/rep.php':
                cursor.execute('Update books_shamela_rep set view_count =? where id=?',
                               (item['view_count'], item['id']))

        if 'pdf_links_details' in item:
            for link in item['pdf_links_details']:
                cursor.execute(
                    "INSERT OR REPLACE INTO pdf_links(link_text,link_value,book_id,repository) values(?,?,?,?)",
                    link + (item['id'], item['repository']))

        return item


class CoverPhotosPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        if 'cover_photo' in item and item['cover_photo']:
            request = scrapy.Request(item['cover_photo'])
            request.meta['book_id'] = item['id']
            return request
        else:
            return []

    def file_path(self, request, response=None, info=None):
        return 'full/%s.jpg' % (request.meta['book_id'])

    def thumb_path(self, request, thumb_id, response=None, info=None):
        return 'thumbs/%s/%s.jpg' % (thumb_id, request.meta['book_id'])
