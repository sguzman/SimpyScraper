import brotli
import bs4
import redis
import requests
from .items_pb2 import *

limit = 1281
red = redis.StrictRedis()
hashmap = red.hgetall('ebooks')


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def get_redis(url):
    key = url.encode() if type(url) == str else url
    if hashmap.get(key) is None:
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        print(f'Inserting Http entry {key} with length {len(comp)}')
        red.hset('ebooks', key, comp)
    else:
        html = hashmap.get(key)
        html = brotli.decompress(html)

    return html


def get_links(i):
    url = f"http://23.95.221.108/page/{i}"

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')
    hrefs = [x.find('a').attrs['href'] for x in arts]

    return [remove_prefix(x, 'https://it-eb.com/') for x in hrefs]


def get_book(path):
    url = f"http://23.95.221.108/{path}"

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.find('h1', class_='post-title').get_text()
    e_id = soup.find('input', {'type': 'hidden', 'name': 'comment_post_ID'})['value']
    host = get_redis(f'http://23.95.221.108/download.php?id={e_id}')

    categories = [x.get_text() for x in soup.find('p', class_='post-btm-cats').findAll('a')]

    details = soup.find('div', class_='book-details').ul
    detail_keys = [x.get_text() for x in details.findAll('span')]
    detail_raw_vals = [x.get_text() for x in details.findAll('li')]
    detail_dict = {k.rstrip(':').lower(): remove_prefix(v, k) for (k, v) in zip(detail_keys, detail_raw_vals)}

    return [arts,
            soup.find('div', class_='book-cover').img['src'],
            detail_dict,
            soup.find('div', class_='entry-inner').get_text(),
            host,
            categories
            ]


# {'isbn-13', 'authors', 'format', 'publication date', 'size', 'publisher', 'isbn-10', 'pages'}
def main():
    some_file = open("items.txt", "w+")
    for i in range(1, limit):
        for path in get_links(i):
            book = get_book(path)
            some_file.write(str(book) + '\n')
    some_file.flush()
    some_file.close()


main()
