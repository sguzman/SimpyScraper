import brotli
import redis
import bs4
import requests
import queue
import threading
import random
from multiprocessing.dummy import Pool as ThreadPool

limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall('ebooks')
pool = ThreadPool(4)
q = queue.LifoQueue()
p = queue.Queue()


def get(url):
    DEFAULT_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/65.0.3325.181 Chrome/65.0.3325.181 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 7.0; Moto G (5) Build/NPPS25.137-93-8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.137 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:59.0) Gecko/20100101 Firefox/59.0",
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0"
    ]

    user_agent = random.choice(DEFAULT_USER_AGENTS)
    headers = {'User-Agent': user_agent}
    p.put(f'Choosing User Agent {user_agent} for request')

    return requests.get(url, headers=headers).text


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_url(text):
    return remove_prefix(text, 'https://it-eb.com/')


def fnflatmap(func, collection):
    new_list = []
    for item in collection:
        val = func(item)
        if type(val) is list:
            new_list = new_list + val
        else:
            new_list.append(val)

    return new_list


def get_redis(url):
    key = url.encode()
    if hashmap.get(key) is None:
        p.put(f'Missed Http cache for url {url}')
        html = get(url)

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        q.put((key, comp))
    else:
        p.put(f'Hit Http cache for url {url}')
        html = hashmap.get(key)
        html = brotli.decompress(html)
        if html is None:
            p.put(f'Body for key {url} is messed up - calling again')
            return get_redis(url)

    return html


def get_links(i):
    url = f"http://23.95.221.108/page/{i}"

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [remove_url(x.find('a').attrs['href']) for x in arts]


def get_book(path):
    url = f"http://23.95.221.108/{path}"

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.find('h1', class_='post-title').get_text()

    details = soup.find('div', class_='book-details').ul
    detail_keys = [x.get_text() for x in details.findAll('span')]
    detail_raw_vals = [x.get_text() for x in details.findAll('li')]
    detail_dict = {k.rstrip(':').lower(): remove_prefix(v, k) for (k, v) in zip(detail_keys, detail_raw_vals)}

    return [arts,
            soup.find('input', attrs={'type': 'hidden', 'name': 'comment_post_ID'})['value'],
            soup.find('div', class_='book-cover').img['src'],
            detail_dict,
            soup.find('div', class_='entry-inner').get_text()
            ]


def get_host(i):
    url = f'http://23.95.221.108/download.php?id={i[1]}'
    return get_redis(url)


def set_redis():
    while q.not_empty:
        key, val = q.get()

        p.put(f'Inserting Http entry {key} with length {len(val)}')
        red.hset('ebooks', key, val)


def print_this():
    while p.not_empty:
        msg = p.get()
        print(msg)


def pmap(func, collection):
    results = pool.map(func, collection)
    return results


def pflatmap(func, collection):
    results = pmap(func, collection)
    return [item for sublist in results for item in sublist]


def main():
    threading.Thread(target=set_redis, daemon=True).start()
    threading.Thread(target=print_this, daemon=True).start()

    ls = pflatmap(get_links, range(1, limit))
    bs = pmap(get_book, ls)
    hs = pmap(get_host, bs)
    p.put(len(hs))


main()
