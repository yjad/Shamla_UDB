import datetime
import re
from urllib.parse import urljoin
from urllib.request import urlopen

import scrapy
#import apsw
import sqlite3

from shamelaScrapper.items import ShamelaOnlineBookInfo


class BooksInfoSpider(scrapy.Spider):
    name = 'books_info'

    def get_repository_from_response(self, url):
        return "/".join(url.split('/')[2:4])

    # def get_latest_book_in_db(self, repository):
    #     print ('from get_latest_book_in_db')
    #     connection = sqlite3.Connection(self.settings.get('SQLITE_PATH', 'data.sqlite'))
    #     cursor = connection.cursor()
    #     if repository == 'shamela.ws/index.php':
    #         cursor.execute('select id,date_added from books_shamela_official order by date_added desc limit 1')
    #     elif repository == 'shamela.ws/rep.php':
    #         cursor.execute('select id,date_added from books_shamela_rep order by date_added desc limit 1')
    #     book = ShamelaOnlineBookInfo()
    #     book['id'], book['date_added'] = cursor.fetchone()
    #     connection.close()
    #     return book
    #
    # def go_to_details_page(self, new_book):
    #     latest_book = self.get_latest_book_in_db(new_book['repository'])
    #     if latest_book['date_added'] > new_book['date_added']:
    #         return False
    #     else:
    #         return True

    #  check if the book-id is in the DB
    def book_already_loaded(self, new_book):
        connection = sqlite3.Connection(self.settings.get('SQLITE_PATH', 'data.sqlite'))
        cursor = connection.cursor()
        if new_book['repository'] == 'shamela.ws/index.php':
            cursor.execute('select exists (select 1 from books_shamela_official where id=? LIMIT 1)', (new_book['id'],))
        elif new_book['repository'] == 'shamela.ws/rep.php':
            cursor.execute('select exists (select 1 from books_shamela_rep where id=? LIMIT 1)', (new_book['id'],))
        b = bool(cursor.fetchone()[0])
        connection.close()
        return b

    def start_requests(self):
        urls = [
            'http://shamela.ws/index.php/search/last/page-1/',
            #'http://shamela.ws/rep.php/search/last/page-1'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_overview(self, book_selector, response):
        book = ShamelaOnlineBookInfo()
        book['id'] = int(book_selector.xpath('a/@href').extract_first().split('/')[-1])
        self.parse_overview_details(book, book_selector.xpath('span[1]/text()').extract_first())
        book['repository'] = self.get_repository_from_response(response.url)
        # book['pdf_link'] = urlopen()
        return book

    def parse(self, response):
        went_to_details = False
        i = 0
        for book_selector in response.css('td.regular-book'):
            book = self.parse_overview(book_selector, response)
            if not self.book_already_loaded(book):
                # read additional data
                request = response.follow(book_selector.xpath('a/@href').extract_first(), self.parse_book_details)
                request.meta['book'] = book
                yield request
            else:
                yield book
            i = i + 1
            if i > 17:
                return
        # if self.folow_next:
        #     next_page = response.xpath("//a[text()='التالي']/@href").extract_first()
        #     if next_page is not None:
        #         yield response.follow(next_page, callback=self.parse)

    def parse_book_details(self, response):
        def select_info_desc_text(info_title):
            return response \
                .xpath("//span[@class='info-item']"
                       "/span[@class='info-title'][contains(text(),'%s')]"
                       "/following-sibling::span/text()" % info_title)

        def select_info_desc_href(info_title):
            return response \
                .xpath("//span[@class='info-item']"
                       "/span[@class='info-title'][contains(text(),'%s')]"
                       "/following-sibling::span/"
                       "a/"
                       "@href" % info_title)

        def select_link_from_img(img_src):
            raw_link = response.xpath(
                # "//div[@style='"
                # "text-align:center;"
                # "letter-spacing:"
                # " 25px;margin:"
                # " 20px 0;']"
                # "/a"
                "//img[contains(@src,'%s')]"
                "/parent::a"
                "/@href" % img_src).extract_first()
            return urljoin(response.url, raw_link) if raw_link else None

        book = response.meta['book']
        # book['id'] = int(response.url.split('/')[-1])
        # book['view_count'] = int(select_info_desc_text('عدد المشاهدات').extract_first())
        # book['date_added'] = parse_date(select_info_desc_text('تاريخ الإضافة').extract_first())
        book['tags'] = ','.join(
            [*map(lambda url: (urljoin(response.url, url)) if url else None,
                  select_info_desc_href('الوسوم').extract())])
        book['rar_link'] = select_link_from_img('bok.png')
        pdf_link = select_link_from_img('pdf.png')
        book['pdf_link'] = pdf_link
        book['online_link'] = select_link_from_img('online.png')
        book['epub_link'] = select_link_from_img('epubd.png')
        book['uploading_user'] = urljoin(response.url,
                                         response.xpath("//a[contains(@href,'user')]/@href")
                                         .extract_first())
        cover_photo = response.xpath("//div[@id='content']/descendant::img[contains(@src,'cover')]/@src").extract_first()
        if cover_photo is not None:
            book['cover_photo'] = cover_photo
        book['time_stamp'] = datetime.datetime.now()

        if (not pdf_link):
            yield book
        else:
            request = response.follow(pdf_link, self.parse_waqfeya)
            request.meta['book'] = book
            yield request

    def parse_waqfeya(self, response):
        table = response.xpath(
            "//span[@class='cattitle'][a[contains(@href,'category.php')]]/ancestor::table/following-sibling::table[1]")
        anchors = table.xpath("descendant::span[@class='postbody']/"
                              "ul/"
                              "descendant::a[not(contains(@href,'shamela'))]")
        book = response.meta['book']
        link_values = anchors.xpath("@href").extract()
        link_texts = anchors.xpath("text()").extract()
        book['pdf_links_details'] = zip(link_texts, link_values)
        cover_photo = table.xpath("descendant::img/@src").extract_first()
        if cover_photo is not None:
            book['cover_photo'] = cover_photo
        yield book

    def parse_overview_details(self, book, string):
        m = book_info_overview_regex.match(string)
        if (m):
            day = int(m.group(1))
            monthText = m.group(2)
            if monthText in arabic_month_names:
                monthNumber = arabic_month_names.index(monthText)
            else:
                raise ValueError('Invalid month name %s' % monthText)
            year = int(m.group(3))
            book['date_added'] = datetime.date(year, monthNumber, day).strftime('%Y-%m-%d')
            book['view_count'] = int(m.group(4))
        else:
            raise ValueError('Invalid format %s' % string)


arabic_month_names = [None, 'يناير',
                      'فبراير',
                      'مارس',
                      'إبريل',
                      'مايو',
                      'يونيو',
                      'يوليو',
                      'أغسطس',
                      'سبتمبر',
                      'أكتوبر',
                      'نوفمبر',
                      'ديسمبر'
                      ]
prog = re.compile(r'(\s*\d{1,2})\s+(\D+)\s+(\d{4})\s+م?\s*')
book_info_overview_regex = re.compile(
    r'^أضيف بتاريخ: (\s*\d{1,2})\s+(\D+)\s+(\d{4})\s+م?\s*-\s*عدد المشاهدات:\s*(\d+)$')


def parse_date(date):
    m = prog.match(date)
    if (m):
        day = int(m.group(1))
        monthText = m.group(2)
        if monthText in arabic_month_names:
            monthNumber = arabic_month_names.index(monthText)
        else:
            raise ValueError('Invalid month name %s' % monthText)
        year = int(m.group(3))
        return datetime.date(year, monthNumber, day).strftime('%Y-%m-%d')
    else:
        raise ValueError('Invalid date format %s' % date)
