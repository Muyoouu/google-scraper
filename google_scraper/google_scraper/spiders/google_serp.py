import scrapy
import json
import datetime
from os import environ
from dotenv import load_dotenv
from urllib.parse import urlencode

# Loads environment variables from ".env"
load_dotenv()

# Prepare scraperAPI request URL for proxy connection
# Request URL must use allowed domain only
def get_url(url):
    payload = {"api_key": environ.get("SCRAPER_API_KEY"), "autoparse": "true", "country_code": "us"}
    proxy_url = "http://api.scraperapi.com/?" + urlencode(payload) + "&url=" + url
    return proxy_url

# Prepare Google query URL
def create_google_url(query):
    return "http://www.google.com/search?" + urlencode({'q':query})


# Spider
class GoogleSerpSpider(scrapy.Spider):
    name = "google_serp"
    
    # Domains to send request into
    allowed_domains = ["api.scraperapi.com"]
    
    # Custom settings for this spider, override settings.py
    custom_settings = {"ROBOTSTXT_OBEY": False, 
                       "LOG_LEVEL": "INFO", 
                       "CONCURRENT_REQUESTS_PER_DOMAIN": 1, 
                       "RETRY_TIMES": 5}
    
    # HTTP request headers
    headers = {"Accept": "application/json",
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
        queries = ["kaesang site:twitter.com"]
        if not environ.get("SCRAPER_API_KEY") or not environ.get("SCRAPEOPS_API_KEY"):
            raise RuntimeError("API_KEY not set")
        for query in queries:
            url = create_google_url(query)
            yield scrapy.Request(url=get_url(url), 
                                 callback=self.parse,
                                 headers=self.headers)

    # Function to parse response obtained
    def parse(self, response):
        results = json.loads(response.text)
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for result in results["organic_results"]:
            item = {
                "title": result["title"],
                "snippet": result["snippet"],
                "link": result["link"],
                "date": dt
            }
            yield item
