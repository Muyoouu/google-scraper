import scrapy
import logging
from datetime import datetime
from google_scraper.items import GoogleSearchResult
from os import environ
from dotenv import load_dotenv
from urllib.parse import urlencode

# Loads environment variables from ".env"
load_dotenv()

# Prepare API request URL for proxy connection
# Request URL must use allowed domain only
def get_url(url):
    payload = {"api_key": environ.get("SCRAPEOPS_API_KEY"), "url": url}
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    
    logging.info("Final URL: %s", proxy_url)
    return proxy_url

# Prepare Google query URL
def create_google_url(query, num=100, start=0):
    url = {
        "q": query,
        "num": num,
        "start": start
    }
    return "https://www.google.com/search?" + urlencode(url)


# Spider
class GoogleSerpSpider(scrapy.Spider):
    name = "google_serp"
    
    # Domains to send request into
    allowed_domains = ["api.scraperapi.com", "proxy.scrapeops.io"]
    
    # Custom settings for this spider, override settings.py
    custom_settings = {"ROBOTSTXT_OBEY": False, 
                       "LOG_LEVEL": "INFO", 
                       "CONCURRENT_REQUESTS_PER_DOMAIN": 1, 
                       "RETRY_TIMES": 5,
                       "SCRAPEOPS_API_KEY": environ.get("SCRAPEOPS_API_KEY")}
    
    # HTTP request headers
    headers = {"Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
               "Accept-Encoding": "gzip, deflate, br",
               "Accept-Language": "en-US,en;q=0.9",
               "Sec-Ch-Ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
               "Sec-Ch-Ua-Mobile": "?0",
               "Sec-Ch-Ua-Platform": "\"Windows\"",
               "Sec-Fetch-Dest": "empty",
               "Sec-Fetch-Mode": "cors",
               "Sec-Fetch-Site": "same-origin",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }

    # Function call when spider crawl request initiated
    def start_requests(self):
        queries = ["naruto"]
        scraped_items = 0
        ITEMS_PER_REQUEST = 100
        DESIRED_ITEMS = 200
        
        if not environ.get("SCRAPER_API_KEY") or not environ.get("SCRAPEOPS_API_KEY"):
            raise RuntimeError("API_KEY not set")
        for query in queries:
            url = create_google_url(query, num=ITEMS_PER_REQUEST)
            yield scrapy.Request(url=get_url(url), callback=self.parse, headers=self.headers, meta={"scraped_items": scraped_items, 
                                                                                                    "query": query, 
                                                                                                    "DESIRED_ITEMS": DESIRED_ITEMS, 
                                                                                                    "ITEMS_PER_REQUEST": ITEMS_PER_REQUEST})

    # Function to parse response obtained
    def parse(self, response):
        query = response.meta["query"]
        scraped_items = response.meta["scraped_items"]
        ITEMS_PER_REQUEST = response.meta["ITEMS_PER_REQUEST"]
        DESIRED_ITEMS = response.meta["DESIRED_ITEMS"]

        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item = GoogleSearchResult()
        results = response.xpath("//h1[contains(text(),'Search Results')]/following-sibling::div[1]/div")
        if not results:
            raise ValueError("Unexpected HTML xpath, parse failed")
        for box in results:
            item["title"] = box.xpath(".//h3/text()").get(),
            item["url"] = box.xpath(".//h3/../@href").get(),
            item["text"] = "".join(box.xpath(".//div[@data-sncf='1']//text()").getall()),
            item["datetime"] = dt
            
            yield item

        # Call request to next page if desired items to be scraped ("DESIRED_ITEMS") not yet scraped
        scraped_items += ITEMS_PER_REQUEST
        if scraped_items < DESIRED_ITEMS:
            url = create_google_url(query, num=ITEMS_PER_REQUEST, start=scraped_items)
            yield scrapy.Request(url=get_url(url), callback=self.parse, headers=self.headers, meta={"scraped_items": scraped_items, 
                                                                                                    "query": query, 
                                                                                                    "DESIRED_ITEMS": DESIRED_ITEMS, 
                                                                                                    "ITEMS_PER_REQUEST": ITEMS_PER_REQUEST})