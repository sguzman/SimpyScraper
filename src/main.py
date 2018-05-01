import brotli
import redis
import bs4
import requests

limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall('ebooks')


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def get_redis(url):
    key = url.encode() if type(url) == str else url
    if hashmap.get(key) is None:
        print(f'Missed Http cache for url {url}')
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        print(f'Inserting Http entry {key} with length {len(comp)}')
        red.hset('ebooks', key, comp)
    else:
        print(f'Hit Http cache for url {url}')
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


def get_rapid_host(url):
    html = get_redis(url)
    soup = bs4.BeautifulSoup(html, 'html.parser')

    details = soup.find('div', class_='file-info').ul
    if details is None:
        return {}

    detail_keys = [x.get_text() for x in details.findAll('span')]
    detail_raw_vals = [x.get_text() for x in details.findAll('li')]
    detail_dict = {k.rstrip(':').lower(): remove_prefix(v, k) for (k, v) in zip(detail_keys, detail_raw_vals)}

    return detail_dict,


def main():
    ls = [x for links in range(1, limit) for x in get_links(links)]
    bs = [get_book(x) for x in ls]
    hs = [get_host(x) for x in bs]
    xs = [get_rapid_host(x) for x in hs]
    for i in xs:
        print(i)


main()
