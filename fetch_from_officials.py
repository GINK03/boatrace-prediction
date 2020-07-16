from concurrent.futures import ProcessPoolExecutor
import requests
from bs4 import BeautifulSoup
import lxml
import re
from dataclasses import dataclass, asdict
import mojimoji
import glob
from pathlib import Path
import gzip
from hashlib import sha224
import sys
import pandas as pd
from tqdm import tqdm
from collections import namedtuple
import numpy as np
import json
import SanrentanParser

def sanitize(x):
    x = re.sub("\s{1,}", "", x)
    x = re.sub("\n", "", x)
    return x


def sanitize2(x):
    x = x.strip()
    x_arr = x.split("\n")
    x_arr = [re.sub("\s{1,}", "", x) for x in x_arr]
    return x_arr


def name_sanitize(x):
    return re.sub("\s{1,}", " ", x)


def get_digest(url):
    return sha224(bytes(url, "utf8")).hexdigest()[:16]


@dataclass
class Record:
    uniq_key: str = ""
    rank: str = ""
    race_time: str = ""
    tansho_odds: str = ""
    fukusho_odds: str = ""
    sanrentan_odds: dict = None
    cup_name: str = ""
    kaijo_name: str = ""
    waku_name: str = ""
    touroku_no: str = ""
    class_name: str = ""
    racer_name: str = ""
    country: str = ""
    age: str = ""
    weight: str = ""
    fnum: str = ""
    lnum: str = ""
    mean_st: str = ""
    zenkoku_win_in_1_prob: str = ""
    zenkoku_win_in_2_prob: str = ""
    zenkoku_win_in_3_prob: str = ""
    touchi_win_in_1_prob: str = ""
    touchi_win_in_2_prob: str = ""
    touchi_win_in_3_prob: str = ""
    mortor_win_in_1_prob: str = ""
    mortor_win_in_2_prob: str = ""
    mortor_win_in_3_prob: str = ""
    boat_win_in_1_prob: str = ""
    boat_win_in_2_prob: str = ""
    boat_win_in_3_prob: str = ""


def get(arg):
    digest, url = arg
    """
    URLが racelist?のものを許可
    url = "https://www.boatrace.jp/owpc/pc/race/racelist?rno=12&jcd=05&hd=20200528"
    """
    uniq_key = url.split("?")[-1]
    try:
        if Path(f"var/work_cache/{digest}").exists():
            return None

        with gzip.open(f"var/htmls/{digest}", "rt") as fp:
            html = fp.read()
        soup = BeautifulSoup(html, "lxml")

        # print(soup.title)

        """ カップ名 """
        cup_name = soup.find("h2").text

        """ 会場名 """
        kaijo_name = soup.find(attrs={"class": "heading2_area"}).find('img').get("alt")

        objs = []
        """ レーサー毎のレコード """
        for record in soup.find_all("tbody", attrs={"class": "is-fs12"}):
            obj = Record()
            obj.uniq_key = uniq_key
            obj.cup_name = cup_name
            obj.kaijo_name = kaijo_name
            waku_name = record.find(attrs={"class": "is-fs14"}).text
            obj.waku_name = mojimoji.zen_to_han(waku_name)

            touroku_class = sanitize(record.find(attrs={"class": "is-fs11"}).text.strip())
            touroku_no, class_name = touroku_class.split("/")
            obj.touroku_no = touroku_no
            obj.class_name = class_name

            racer_name = record.find(attrs={"class": "is-fs18"}).text.strip()
            obj.racer_name = name_sanitize(racer_name)

            country_age_weight = sanitize(record.find_all(attrs={"class": "is-fs11"})[1].text.strip())
            country, age, weight = country_age_weight.split("/")
            # print(country, age, weight)
            obj.country = country
            obj.age = age
            obj.weight = weight

            fnum_lnum_meanst = record.find_all(attrs={"class": "is-lineH2"})[0].text
            fnum, lnum, mean_st = sanitize2(fnum_lnum_meanst)
            # print(fnum, lnum, mean_st)
            obj.fnum = fnum
            obj.lnum = lnum
            obj.mean_st = mean_st

            zenkoku_raw = record.find_all(attrs={"class": "is-lineH2"})[1].text
            zenkoku_win_in_1_prob, zenkoku_win_in_2_prob, zenkoku_win_in_3_prob = sanitize2(zenkoku_raw)
            # print(zenkoku_win_in_1_prob, zenkoku_win_in_2_prob, zenkoku_win_in_3_prob)
            obj.zenkoku_win_in_1_prob = zenkoku_win_in_1_prob
            obj.zenkoku_win_in_2_prob = zenkoku_win_in_2_prob
            obj.zenkoku_win_in_3_prob = zenkoku_win_in_3_prob

            touchi_raw = record.find_all(attrs={"class": "is-lineH2"})[2].text
            touchi_win_in_1_prob, touchi_win_in_2_prob, touchi_win_in_3_prob = sanitize2(touchi_raw)
            # print(touchi_win_in_1_prob, touchi_win_in_2_prob, touchi_win_in_3_prob)
            obj.touchi_win_in_1_prob = touchi_win_in_1_prob
            obj.touchi_win_in_2_prob = touchi_win_in_2_prob
            obj.touchi_win_in_3_prob = touchi_win_in_3_prob

            mortor_raw = record.find_all(attrs={"class": "is-lineH2"})[3].text
            mortor_win_in_1_prob, mortor_win_in_2_prob, mortor_win_in_3_prob = sanitize2(mortor_raw)
            # print(mortor_win_in_1_prob, mortor_win_in_2_prob, mortor_win_in_3_prob)
            obj.mortor_win_in_1_prob = mortor_win_in_1_prob
            obj.mortor_win_in_2_prob = mortor_win_in_2_prob
            obj.mortor_win_in_3_prob = mortor_win_in_3_prob

            boat_raw = record.find_all(attrs={"class": "is-lineH2"})[4].text
            boat_win_in_1_prob, boat_win_in_2_prob, boat_win_in_3_prob = sanitize2(boat_raw)
            # print(boat_win_in_1_prob, boat_win_in_2_prob, boat_win_in_3_prob )
            obj.boat_win_in_1_prob = boat_win_in_1_prob
            obj.boat_win_in_2_prob = boat_win_in_2_prob
            obj.boat_win_in_3_prob = boat_win_in_3_prob

            objs.append(obj)

        suffix_param = url.split("?").pop()

        result_url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?{suffix_param}"

        if Path(f"var/htmls/{get_digest(result_url)}").exists():
            with gzip.open(f"var/htmls/{get_digest(result_url)}", "rt") as fp:
                html = fp.read()
        else:
            with requests.get(result_url) as r:
                html = r.text
            with gzip.open(f"var/htmls/{get_digest(result_url)}", "wt") as fp:
                fp.write(html)
        soup = BeautifulSoup(html)
        table = soup.find(attrs={"class": "is-w495"})
        # broken file handling
        if table is None:
            if "レース中止" in soup.find_all(attrs={"class": "title12"})[-1].text:
                return None
            print(f"err {result_url}")
            return None
        for tbody in table.find_all("tbody"):
            rank = mojimoji.zen_to_han(tbody.find_all("td")[0].text.strip())
            waku_name = mojimoji.zen_to_han(tbody.find_all("td")[1].text.strip())
            race_time = mojimoji.zen_to_han(tbody.find_all("td")[3].text.strip())
            # print(rank, waku_name)
            for obj in objs:
                if obj.waku_name == waku_name:
                    obj.rank = rank
                    obj.race_time = race_time

        odds_url = f"https://www.boatrace.jp/owpc/pc/race/oddstf?{suffix_param}"
        if Path(f"var/htmls/{get_digest(odds_url)}").exists():
            with gzip.open(f"var/htmls/{get_digest(odds_url)}", "rt") as fp:
                html = fp.read()
        else:
            with requests.get(result_url) as r:
                html = r.text
            with gzip.open(f"var/htmls/{get_digest(odds_url)}", "wt") as fp:
                fp.write(html)

        soup = BeautifulSoup(html)
        odds_table = soup.find_all(attrs={"class": "is-w495"})[0]

        TanshoOdds = namedtuple("TanshoOdds", ["waku_name", "name", "odds"])
        tansho_oddses = [TanshoOdds(*[td.text for td in tr.find_all("td")]) for tr in odds_table.find_all("tr") if len(tr.find_all("td")) == 3]
        for tansho_odds in tansho_oddses:
            for obj in objs:
                if obj.waku_name == tansho_odds.waku_name:
                    obj.tansho_odds = tansho_odds.odds

        odds_table = soup.find_all(attrs={"class": "is-w495"})[1]
        FukushoOdds = namedtuple("FukushoOdds", ["waku_name", "name", "odds"])
        fukusho_oddses = [FukushoOdds(*[td.text for td in tr.find_all("td")]) for tr in odds_table.find_all("tr") if len(tr.find_all("td")) == 3]
        for fukusho_odds in fukusho_oddses:
            for obj in objs:
                if obj.waku_name == fukusho_odds.waku_name:
                    obj.fukusho_odds = fukusho_odds.odds

        odds_url = f"https://www.boatrace.jp/owpc/pc/race/odds3t?{suffix_param}"
        if Path(f"var/htmls/{get_digest(odds_url)}").exists():
            with gzip.open(f"var/htmls/{get_digest(odds_url)}", "rt") as fp:
                html = fp.read()
        else:
            with requests.get(result_url) as r:
                html = r.text
            with gzip.open(f"var/htmls/{get_digest(odds_url)}", "wt") as fp:
                fp.write(html)
        dd = SanrentanParser.get_dd(html)
        for waku_name, sanrentan_odds in dd.items():
            for obj in objs:
                if obj.waku_name == waku_name:
                    obj.sanrentan_odds = sanrentan_odds

        # print(ret)
        # for obj in objs:
        #    print(asdict(obj))
        with open(f"var/work_cache/{digest}", "w") as fp:
            json.dump([asdict(x) for x in objs], fp, indent=2, ensure_ascii=False)
        # return objs
    except Exception as exc:
        tb_lineno = sys.exc_info()[2].tb_lineno
        print(exc, url, soup.title, tb_lineno)


def get_wrap(chunk):
    objs = []
    for arg in chunk:
        ret = get(arg)
        if ret is None:
            continue
        objs.append(ret)
    return objs

def _load_digest_url_files(url_def_file):
    digest = Path(url_def_file).name
    url = gzip.open(url_def_file, "rt").read()
    if re.search("https://www.boatrace.jp/owpc/pc/race/racelist?", url) is None:
        return None
    return (digest, url)


def load_digest_url_files():

    url_def_files = glob.glob("var/urls/*")
    digest_urls = []
    with ProcessPoolExecutor(max_workers=16) as exe:
        for ret in tqdm(exe.map(_load_digest_url_files, url_def_files), total=len(url_def_files), desc="load_digest_url_files..."):
            if ret is None:
                continue
            digest_urls.append(ret)
    
    x = np.array(digest_urls)
    np.random.shuffle(x)
    N = 100
    x = x[:N*(len(x)//N)]
    x = x.reshape((len(x)//N, N, 2))
    return x


if __name__ == "__main__":

    chunks = load_digest_url_files()
    with ProcessPoolExecutor(max_workers=16) as exe:
        for _ in tqdm(exe.map(get_wrap, chunks.tolist()), total=len(chunks), desc="working..."):
            _
    """
    objs = []
            for ret in rets:
                objs += [asdict(r) for r in ret]

    pd.DataFrame(objs).to_csv("var/collect_data.csv.back", index=None)
    """
