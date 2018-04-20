import brotli
import redis
import bs4
import requests
from multiprocessing.dummy import Pool as ThreadPool

base = "http://23.95.221.108/page/"
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
        print('Missed Http cache for url %s' % url)
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        to_input = [key, comp]

        html = [html, to_input]
    else:
        print('Hit Http cache for url %s' % url)
        html = hashmap.get(key)
        html = [brotli.decompress(html), None]

    return html


def get_links(i):
    url = base + str(i)

    html, http = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [[remove_url(x.find('a').attrs['href']) for x in arts], http]


def get_book(i):
    url = base + str(i)

    html, http = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.find('h1.post-title').get_text()
    print(arts)

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

    print('Got %d new Http entries' % len(new_entries))
    for pair in new_entries:
        key = pair[0]
        val = pair[1]

        print('Inserting Http entry %s with length %d' % (key, len(val)))
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
