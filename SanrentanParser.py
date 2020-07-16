from itertools import permutations
import requests
from bs4 import BeautifulSoup
import numpy as np

def get_dd(html):
    soup = BeautifulSoup(html)

    ms = []
    for i, j, k in permutations([1, 2, 3, 4, 5, 6], 3):
        ms.append((i, j, k))

    ops = []
    for op in soup.find_all(attrs={"class": "oddsPoint"}):
        ops.append(op.text)
    ops = np.array(ops).reshape((20, 6))

    ops = ops.T

    dd = {key: {} for key in range(1, 7)}
    for m, op in zip(ms, ops.flatten()):
        # print(m, op)
        key = m[0]
        m_str = "_".join(map(str, m))
        dd[key][m_str] = op

    # print(dd)
    return dd

if __name__ == "__main__":
    with requests.get("https://www.boatrace.jp/owpc/pc/race/odds3t?rno=12&jcd=05&hd=20200528") as r:
        html = r.text
    get_dd(html)
