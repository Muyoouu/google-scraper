import scrapy
import logging
import json
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
    payload = {"api_key": environ.get("SCRAPEOPS_API_KEY"), "auto_extract": "google", "url": url}
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    
    logging.info("Final URL: %s", proxy_url)
    return proxy_url

# Prepare Google query URL
def create_google_url(query, num=100, start=0):
    url = {
        "q": query,
        "num": num,
        "hl": "en",
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
        queries = ["apple fact"]
        current_page = 0
        ITEMS_PER_REQUEST = 100
        TARGET_PAGE = 2
        
        if not environ.get("SCRAPER_API_KEY") or not environ.get("SCRAPEOPS_API_KEY"):
            raise RuntimeError("API_KEY not set")
        for query in queries:
            url = create_google_url(query, num=ITEMS_PER_REQUEST)
            yield scrapy.Request(url=get_url(url), callback=self.parse_json, headers=self.headers, meta={"current_page": current_page, 
                                                                                                    "query": query, 
                                                                                                    "TARGET_PAGE": TARGET_PAGE, 
                                                                                                    "ITEMS_PER_REQUEST": ITEMS_PER_REQUEST})

    # Function to parse response obtained
    def parse(self, response):
        query = response.meta["query"]
        current_page = response.meta["current_page"]
        ITEMS_PER_REQUEST = response.meta["ITEMS_PER_REQUEST"]
        TARGET_PAGE = response.meta["TARGET_PAGE"]

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

        # Call request to next page if desired items to be scraped ("TARGET_PAGE") not yet scraped
        current_page += 1
        if current_page < TARGET_PAGE:
            url = create_google_url(query, num=ITEMS_PER_REQUEST, start=current_page*ITEMS_PER_REQUEST)
            yield scrapy.Request(url=get_url(url), callback=self.parse, headers=self.headers, meta={"current_page": current_page, 
                                                                                                    "query": query, 
                                                                                                    "TARGET_PAGE": TARGET_PAGE, 
                                                                                                    "ITEMS_PER_REQUEST": ITEMS_PER_REQUEST})
    
    # Function to parse API autoparse response (json) obtained
    def parse_json(self, response):
        query = response.meta["query"]
        current_page = response.meta["current_page"]
        ITEMS_PER_REQUEST = response.meta["ITEMS_PER_REQUEST"]
        TARGET_PAGE = response.meta["TARGET_PAGE"]

        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item = GoogleSearchResult()
        results = json.loads(response.text)
        for box in results["data"]["organic_results"]:
            item["title"] = box.get("title","Empty"),
            item["url"] = box.get("link","Empty"),
            item["text"] = box.get("snippet","Empty"),
            item["datetime"] = dt
            
            yield item

        # Call request to next page if desired pages to be scraped ("TARGET_PAGE") not yet scraped
        current_page += 1
        if current_page < TARGET_PAGE:
            next_page = results["data"]["pagination"].get("next_page_url")
            if next_page:
                yield scrapy.Request(url=get_url(next_page), callback=self.parse_json, headers=self.headers, meta={"current_page": current_page, 
                                                                                                                    "query": query, 
                                                                                                                    "TARGET_PAGE": TARGET_PAGE, 
                                                                                                                    "ITEMS_PER_REQUEST": ITEMS_PER_REQUEST})
            else:
                logging.info("No next page to be scraped, current_page:%s, pages_count:%s", current_page, results["data"]["pagination"].get("pages_count"))
        else:
            logging.info("Target page reached, current_page:%s, target_page:%s, pages_count:%s", current_page, TARGET_PAGE, results["data"]["pagination"].get("pages_count"))