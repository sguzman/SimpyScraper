import brotli
import bs4
import redis
import requests
import os

limit = 1290
red = redis.StrictRedis()
hash_map = red.hgetall('ebooks')


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):].replace('/', '')
    return text


def get_redis(url):
    key = url.encode() if type(url) == str else url
    if hash_map.get(key) is None:
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        print(f'Inserting Http entry {key} with length {len(comp)}')
        red.hset('ebooks', key, comp)
    else:
        html = hash_map.get(key)
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
    return get_redis(url)


def write(file_name, content):
    fd = open(file_name, 'w')
    fd.write(content)
    fd.flush()
    fd.close()


def main():
    dir_name = "./txt/"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    for i in range(1, limit + 1):
        for path in get_links(i):
            file = f'{dir_name}{path}.txt'
            if not os.path.exists(file):
                html = get_book(path)
                write(file, html)




main()
