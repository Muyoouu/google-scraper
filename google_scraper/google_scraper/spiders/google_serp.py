import scrapy
import json
import datetime
from os import environ
from dotenv import load_dotenv
from urllib.parse import urlparse
from urllib.parse import urlencode

# Loads environment variables from ".env"
load_dotenv()

# Prepare scraperAPI request URL for proxy connection
def get_url(url):
    payload = {"api_key": environ.get("SCRAPER_API_KEY"), "url": url, "autoparse": "true", "country_code": "us"}
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
        if not environ.get("SCRAPER_API_KEY") or not environ.get("SCRAPEOPS_API_KEY"):
            raise RuntimeError("API_KEY not set")
        for query in queries:
            url = create_google_url(query)
            yield scrapy.Request(get_url(url), callback=self.parse, meta={"pos": 0})
        return super().start_requests()

    def parse(self, response):
        results = json.loads(response.text)
        pos = response.meta["pos"]
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for result in results["organic_results"]:
            item = {
                "title": result["title"],
                "snippet": result["snippet"],
                "link": result["link"],
                "position": pos,
                "date": dt
            }
            pos += 1
            yield item
