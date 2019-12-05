# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ShamelaOnlineBookInfo(scrapy.Item):
    id = scrapy.Field()
    view_count = scrapy.Field()
    date_added = scrapy.Field()
    tags = scrapy.Field()
    rar_link = scrapy.Field()
    pdf_link = scrapy.Field()
    pdf_links_details=scrapy.Field()
    epub_link = scrapy.Field()
    online_link = scrapy.Field()
    uploading_user = scrapy.Field()
    repository = scrapy.Field()
    cover_photo = scrapy.Field()

