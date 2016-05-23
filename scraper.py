import scrapy
from scrapy.crawler import CrawlerProcess
import scraperwiki
from urlparse import urljoin
import calendar
from datetime import datetime

class DioxinSpider(scrapy.Spider):
    name = "dioxin"
    def start_requests(self):
        yield scrapy.Request('http://www.aqhi.gov.hk/en/download/air-quality-reports2985.html?showall=&start=3', callback=self.parse_list)


    def parse_list(self, response):
        table = response.xpath('//div[@class=\'item-page\']/table')
        rows = table.xpath("tbody/tr")
        for row in rows[1:]:
            link = urljoin(response.url, row.xpath(".//a/@href").extract()[0])
            print "crawling %s" % (link)
            yield scrapy.Request(link, callback=self.parse_summary)

    def parse_summary(self, response):
        table = response.xpath('//div[@class=\'item-page\']/table//table')
        h2    = response.xpath('//div[@class=\'item-page\']/h2/text()').extract()[0].strip()
        print "[%s]" % h2
        year = int(h2.split(" ")[-1].strip())
        rows = table.xpath(".//tr")
        last_month = 0
        for row in rows[2:]:
            cells = row.xpath(".//td")
            #print len(cells)
            if len(cells) == 4:
                month_str = cells[0].xpath(".//span/text()").extract()[0].strip()
                last_month = list(calendar.month_name).index(month_str)
                cells = cells[1:]
            if len(cells) == 3:
                sampling_date_str = cells[0].xpath(".//span/text()").extract()[0].strip()
                if sampling_date_str == "Annual Average":
                    continue
                try:
                    central_str       = " ".join(cells[1].xpath(".//text()").extract()).strip()
                    tsuen_wan_str       = " ".join(cells[2].xpath(".//text()").extract()).strip()
                except:
                    print "Error in finding cell text %d,%d,%s" % (year, last_month, cells[2].xpath(".//span/text()").extract())
                try:
                    sampling_date     = None if sampling_date_str in ["--", ""] else datetime.strptime(sampling_date_str, "%d-%m-%Y")
                    central = None if central_str in ["--", ""] else float(central_str)
                    tsuen_wan = None if tsuen_wan_str in ["--", ""] else float(tsuen_wan_str)
                    d = {"central": central, "tsuen_wan": tsuen_wan, "sampling_date": sampling_date, 'month':last_month, 'year': year}
                    print d
                    scraperwiki.sqlite.save(unique_keys=["year", "month"], data=d)
                except:
                    print "Error in conversion %d,%d,%s,%s,%s" % (year,last_month,sampling_date_str, central_str, tsuen_wan_str)

 

scraperwiki.sqlite.execute("DROP TABLE IF EXISTS data")
process = CrawlerProcess()
process.crawl(DioxinSpider)
process.start()

 

