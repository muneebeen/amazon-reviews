##############################################################################################################
#
#  We will get product reviews based upon ASIN which will be provided.
#  More than 1 ASIN can be provided so we will use start_request method to start the crawling.
#  We will also send headers which will be placed in settings.py.
#  Headers should only be passed in dictionary format so we will use a scrapy package 'scraper_helper' to do that.
# 'AUTOTHROTTLE_ENABLED = True' in setting.py so it will create random delays between page scrapings and
# if we abort the main it will store the results in our desired file/output.
#
#################################################################################################################
import gspread as gspread
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import time

url = 'https://www.amazon.com/product-reviews/{}'
ASIN_CODES = ['B08MWP6CK4', 'B08615TQ9C']


class AmazonReviewScriptSpider(scrapy.Spider):
    name = 'amazon_review-script'
    all_reviews = list()
    gc = gspread.service_account(filename="creds.json")
    sh = gc.open('amazon-reviews').sheet1

    def start_requests(self):
        for asin_code in ASIN_CODES:
            review_url = url.format(asin_code)
            yield scrapy.Request(review_url, meta={'asin': asin_code})

    def parse(self, response):
        res = response.css('div#cm_cr-review_list div[data-hook="review"]')
        for review in res:
            asin_code = response.meta.get('asin')
            review = {
                'name': review.css('span.a-profile-name ::text').get(),
                'stars': review.css('span.a-icon-alt::text').get(),
                'title': review.css('a[data-hook = "review-title"] > span::text').get(),
                'reviews': review.css('[data-hook = "review-body"] > span::text').get(),
                'ASIN': asin_code
            }
            yield review
            self.all_reviews.append(review)

        try:
            next_page = res.xpath('//a[contains(text(),"Next page")]/@href').get()
            if next_page:
                # urljoin is use to get the absolute url.
                yield scrapy.Request(response.urljoin(next_page))
        except:
            print("No Next Page Found")

    def close(spider, reason):
        # dataframe = pd.DataFrame(spider.all_reviews)
        # dataframe.to_csv('All_Reviews')
        spider.sh.append_row(
            ['Name', 'Stars', 'Title', 'Review', 'ASIN'])
        row_index = 1
        for data in spider.all_reviews:
            row_index += 1
            spider.sh.insert_row(list(data.values()), row_index)
            time.sleep(3)

process = CrawlerProcess()
process.crawl(AmazonReviewScriptSpider)
process.start()
