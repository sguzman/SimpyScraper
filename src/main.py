import brotli
import redis
import bs4
import requests
import queue
from multiprocessing.dummy import Pool as ThreadPool

base = "http://23.95.221.108/page/"
redisHash = 'ebooks'
limit = 1268
red = redis.StrictRedis()
hashmap = red.hgetall(redisHash)


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


def get_redis(url, new_entries):
    key = str.encode(url)
    if hashmap.get(key) is None:
        print('Missed Http cache for url %s' % url)
        html = requests.get(url).text

        comp = brotli.compress(html.encode(), brotli.MODE_TEXT)
        to_input = [key, comp]

        new_entries.put(to_input)
    else:
        print('Hit Http cache for url %s' % url)
        html = hashmap.get(key)
        html = brotli.decompress(html)

    return html


def get_links(i, new_entries):
    url = base + str(i)

    html = get_redis(url, new_entries)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [remove_url(x.find('a').attrs['href']) for x in arts]


def pmap(func, collection, cores=4):
    pool = ThreadPool(cores)
    new_entries = queue.LifoQueue()

    results = pool.map(lambda x: func(x, new_entries), collection)
    new_entries.join()
    print('Got %d Http entries' % new_entries.qsize())
    for i in range(new_entries.qsize()):
        pair = new_entries.get()
        key = pair[0]
        val = pair[1]

        print('Inserting Http entry %s with length %d' % (key, len(val)))
        red.hset(redisHash, key, val)

    return results


def pflatmap(func, collection, cores=4):
    results = pmap(func, collection, cores)
    return [item for sublist in results for item in sublist]


def main():
    ls = pflatmap(get_links, range(1, limit))
    print(len(ls))


main()
