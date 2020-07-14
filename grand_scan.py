import asyncio
import glob
from concurrent.futures import ProcessPoolExecutor
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
from hashlib import sha224
from pathlib import Path
import gzip
from tqdm import tqdm
import random

seed = "https://www.boatrace.jp/owpc/pc/race/index"
domain = "https://www.boatrace.jp"

def init():
    Path(f"var/htmls").mkdir(exist_ok=True, parents=True)
    Path(f"var/urls").mkdir(exist_ok=True, parents=True)

init()


def get_digest(url):
    return sha224(bytes(url, "utf8")).hexdigest()[:16]


def save_with_digest(url, html):
    digest = get_digest(url)
    with gzip.open(f"var/htmls/{digest}", "wt") as fp:
        fp.write(html)
    with gzip.open(f"var/urls/{digest}", "wt") as fp:
        fp.write(url)


def get_urls_from_html(html):
    soup = BeautifulSoup(html, "lxml")
    urls = set()
    for a in soup.find_all("a", {"href": re.compile(f"[^{domain}|^/]")}):
        url = a.get("href")
        if re.search(f"^/", url):
            url = domain + url
        if urlparse(url).netloc != "www.boatrace.jp":
            continue
        urls.add(url)
    return urls


def get(url):
    try:
        digest = get_digest(url)
        if Path(f"var/htmls/{digest}").exists():
            return set()

        with requests.get(url) as r:
            html = r.text
        save_with_digest(url, html)
        urls = get_urls_from_html(html)
        return urls
    except Exception as exc:
        print(exc)
        return set()


def run(urls):
    asyncio.run(get(urls))


if __name__ == "__main__":
    filenames = glob.glob("var/htmls/*")
    if filenames == []:
        urls = get(seed)
    else:
        urls = set()
        for filename in tqdm(random.sample(filenames, 10000)):
            with gzip.open(filename, "rt") as fp:
                urls |= get_urls_from_html(fp.read())

    for i in range(10):
        with ProcessPoolExecutor(max_workers=100) as exe:
            next_urls = set()
            for _next_urls in exe.map(get, urls):
                next_urls |= _next_urls
            urls = next_urls
