from scrapy import cmdline
cmdline.execute("scrapy crawl books_info -a folow_next=False".split())