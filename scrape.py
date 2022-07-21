from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as soup
import time
import pandas as pd
from typing import List


def get_category_links(url: str, n: int) -> List[str]:

    # Get all available pages urls in particular category
    # '?n=1' - filter query to select apartments in Yerevan
    cat_urls = [url + str(i) + '?n=1' for i in range(1, n)]

    links = []
    a_tags = []
    for url in cat_urls:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read().decode('utf-8')
        page_soup = soup(webpage, "html.parser")
        a_tags += list(page_soup.select('div.gl a'))
        # time.sleep(2)

    for a in a_tags:
        full_url = 'https://list.am/en' + a['href']
        if full_url not in links:
            links.append(full_url)

    return links

