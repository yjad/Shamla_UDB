# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

#import apsw
import datetime
from scrapy.exceptions import DropItem
import sqlite3
import scrapy
from scrapy.pipelines.images import ImagesPipeline, FilesPipeline

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
        self.connection = sqlite3.Connection(self.db_file)
        cursor = self.connection.cursor()
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {TABLE_SHAMELA_OFFICIAL}'
                       '(id INTEGER NOT NULL PRIMARY KEY,'
                       'time_stamp TEXT ,'
                       'view_count INTEGER,'
                       'date_added TEXT,'
                       'tags TEXT,'
                       'rar_link TEXT,'
                       'pdf_link TEXT,'
                       'online_link TEXT,'
                       'epub_link TEXT)')
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {TABLE_SHAMELA_REP}'
                       '(id INTEGER NOT NULL PRIMARY KEY,'
                        'time_stamp TEXT ,'
                       'view_count INTEGER,'
                       'date_added TEXT,'
                       'rar_link TEXT,'
                       'uploading_user TEXT)')
        cursor.execute("CREATE TABLE IF NOT EXISTS 'pdf_links'"
                       "(book_id INTEGER NOT NULL,"
                       "time_stamp TEXT,"
                       "repository TEXT, "
                       "link_text TEXT, "
                       "link_value TEXT ,"
                       "UNIQUE (book_id, link_text,link_value))")

    def close_spider(self, spider):
        cursor = self.connection.cursor()
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS `official_date_added` ON `books_shamela_official` (`date_added` DESC)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS `repo_date_added` ON `books_shamela_rep` (`date_added` DESC)")
        cursor.close()
        #self.connection.close(True)
        self.connection.close()
        pass

    def process_item(self, item, spider):
        cursor = self.connection.cursor()
        if 'rar_link' in item:
            if item['repository'] == 'shamela.ws/index.php':
                count = cursor.execute("INSERT INTO " + TABLE_SHAMELA_OFFICIAL + " VALUES("
                                                                                    ":id,"
                                                                                    ":time_stamp, "
                                                                                    ":view_count,"
                                                                                    ":date_added,"
                                                                                    ":tags,"
                                                                                    ":rar_link,"
                                                                                    ":pdf_link,"
                                                                                    ":online_link,"
                                                                                    ":epub_link)"
                               , dict(item))
                self.connection.commit()
            elif item['repository'] == 'shamela.ws/rep.php':
                cursor.execute("INSERT INTO " + TABLE_SHAMELA_REP + " VALUES("
                                                                               ":id,"
                                                                               ":time_stamp, "
                                                                               ":view_count,"
                                                                               ":date_added,"
                                                                               ":rar_link,"
                                                                               ":uploading_user)"
                                                                , dict(item))
                self.connection.commit()
            if 'pdf_links_details' in item:
                for link in item['pdf_links_details']:
                    cursor.execute(
                        "INSERT OR REPLACE INTO pdf_links(link_text,link_value,book_id,repository, time_stamp) "
                        "VALUES(?,?,?,?,?)",
                        link + (item['id'], item['repository'], datetime.datetime.now()))
                    self.connection.commit()
            return item

        else: # book already loaded, just update View Count
            if item['repository'] == 'shamela.ws/index.php':
                cursor.execute(f"UPDATE {TABLE_SHAMELA_OFFICIAL} SET view_count ={item['view_count']} WHERE id={item['id']}")
            elif item['repository'] == 'shamela.ws/rep.php':
                cursor.execute(f"UPDATE {TABLE_SHAMELA_REP} SET view_count ={item['view_count']} WHERE id={item['id']}")
            self.connection.commit()
            raise DropItem()

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


class rarFilePipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        print ('in rar files pipline: ', item['rar_link'])
        if 'rar_link' in item and item['rar_link']:
            request = scrapy.Request(item['rar_link'])
            request.meta['book_id'] = item['id']
            return request
        else:
            return []

    def file_path(self, request, response=None, info=None):
         return f"{request.meta['book_id']}.rar"

    def item_completed(self, results, item, info):
        print ('results\n', results)

