import brotli
import redis
import bs4
import requests
from multiprocessing.dummy import Pool as ThreadPool

baseLink = "http://23.95.221.108/page/"
baseBook = "http://23.95.221.108/"

redisHash = 'ebooks'
limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall(redisHash)
coreLimit = 8
pool = ThreadPool(coreLimit)


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
    arts = soup.find('h1', class_='post-title')
    print(arts)
    arts = arts.get_text()

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
