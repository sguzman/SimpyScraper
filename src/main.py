import brotli
import redis
import bs4
import requests
from multiprocessing.dummy import Pool as ThreadPool

limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall('ebooks')
pool = ThreadPool(8)


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
        print(f'Missed Http cache for url {url}')
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        to_input = [key, comp]

        html = [html, to_input]
    else:
        print(f'Hit Http cache for url {url}')
        html = hashmap.get(key)
        html = [brotli.decompress(html), None]

    return html


def get_links(i):
    url = baseLink + str(i)

    html, http = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [[remove_url(x.find('a').attrs['href']) for x in arts], http]


def get_book(i):
    url = baseBook + str(i)

    html, http = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.find('h1', class_='post-title').get_text()

    details = soup.find('div', class_='book-details').ul
    detail_keys = [x.get_text() for x in details.findAll('span')]
    detail_raw_vals = [x.get_text() for x in details.findAll('li')]
    detail_dict = {k.rstrip(':').lower(): remove_prefix(v, k) for (k, v) in zip(detail_keys, detail_raw_vals)}

    arts = [arts,
            soup.find('input', attrs={'type': 'hidden', 'name': 'comment_post_ID'})['value'],
            soup.find('div', class_='book-cover').img['src'],
            detail_dict,
            soup.find('div', class_='entry-inner').get_text()
            ]

    return [arts, http]


def pmap(func, collection):
    raw_results = pool.map(func, collection)
    results = []
    new_entries = []
    for i in raw_results:
        result = i[0]
        new_entry = i[1]
        results.append(result)
        if new_entry is not None:
            new_entries.append(new_entry)

    print(f'Got {len(new_entries)} new Http entries')
    for pair in new_entries:
        key = pair[0]
        val = pair[1]

        print(f'Inserting Http entry {key} with length {len(val)}')
        red.hset(redisHash, key, val)

    return results


def pflatmap(func, collection):
    results = pmap(func, collection)
    return [item for sublist in results for item in sublist]


def main():
    ls = pflatmap(get_links, range(1, limit))
    bs = pmap(get_book, ls)
    print(len(bs))


main()
