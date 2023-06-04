import scrapy
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
from urllib.parse import urlencode

# Loads environment variables from ".env"
load_dotenv()

# Prepare scraperAPI request URL for proxy connection
def get_url(url):
    payload = {"api_key": os.environ.get("SCRAPER_API_KEY"), "url": url, "autoparse": "true", "country_code": "us"}
    proxy_url = "https://api.scraperapi.com/?" + urlencode(payload)
    return proxy_url

# Prepare Google query URL
def create_google_url(query, site=""):
    google_dict = {"q": query, "num": 100}
    if site:
        web = urlparse(site).netloc
        google_dict["as_sitesearch"] = web
    return "https://www.google.com/search?" + urlencode(google_dict)


class GoogleSerpSpider(scrapy.Spider):
    name = "google_serp"
    allowed_domains = ["api.scraperapi.com"]
    start_urls = ["https://api.scraperapi.com"]

    # Function call when spider request initiated
    def start_requests(self):
        queries = ["scrapy", "beautifulsoup"]
        if not os.environ.get("SCRAPER_API_KEY") or not os.environ.get("SCRAPEOPS_API_KEY"):
            raise RuntimeError("API_KEY not set")
        for query in queries:
            url = create_google_url(query)
            yield scrapy.Request(get_url(url), callback=self.parse, meta={"pos": 0})
        return super().start_requests()

    def parse(self, response):
        pass
