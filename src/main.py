import brotli
import redis
import bs4
import requests
import queue
import threading
from multiprocessing.dummy import Pool as ThreadPool

limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall('ebooks')
pool = ThreadPool(8)
q = queue.LifoQueue()
p = queue.Queue()


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
    key = str.encode(url)
    if hashmap.get(key) is None:
        p.put(f'Missed Http cache for url {url}')
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        to_input = [key, comp]

        html = [html, to_input]
    else:
        p.put(f'Hit Http cache for url {url}')
        html = hashmap.get(key)
        html = [brotli.decompress(html), None]

    return html


def get_links(i):
    url = f"http://23.95.221.108/page/{i}"

    html, http = get_redis(url)
    q.put(http)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [remove_url(x.find('a').attrs['href']) for x in arts]


def get_book(path):
    url = f"http://23.95.221.108/{path}"

    html, http = get_redis(url)
    q.put(http)

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
        key, val = q.get(timeout=100)

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
