import signal
import sys
import redis
import bs4
import requests


def init():
    def signal_handler():
        print('You pressed Ctrl+C! - quiting...')
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)


init()


class SGlobals:
    base = "http://23.95.221.108/page/"
    redisHash = 'ebooks'
    limit = 1268


class SUtil:
    @staticmethod
    def remove_prefix(text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text


    @staticmethod
    def remove_url(text):
        return SUtil.remove_prefix(text, 'https://it-eb.com/')


    @staticmethod
    def fnflatmap(func, collection):
        new_list = []
        for item in collection:
            val = func(item)
            if type(val) is list:
                new_list = new_list + val
            else:
                new_list.append(val)

        return new_list


class SRedis:
    red = redis.StrictRedis()
    hashmap = red.hgetall(SGlobals.redisHash)


    @staticmethod
    def get_redis(url):
        key = str.encode(url)
        if SRedis.hashmap.get(key) is None:
            print('Missed Http cache for url %s' % url)
            html = requests.get(url).text
            SRedis.red.hset(SGlobals.redisHash, url, html)
        else:
            print('Hit Http cache for url %s' % url)
            html = SRedis.hashmap.get(key)

        return html

    @staticmethod
    def get_links(i):
        url = SGlobals.base + str(i)

        html = SRedis.get_redis(url)

        soup = bs4.BeautifulSoup(html, 'html.parser')
        arts = soup.findAll('article')

        return [SUtil.remove_url(x.find('a').attrs['href']) for x in arts]


    @staticmethod
    def links():
        return SUtil.fnflatmap(SRedis.get_links, range(1, SGlobals.limit))


    @staticmethod
    def get_book(path):
        url = SGlobals.base + path

        html = SRedis.get_redis(url)

        soup = bs4.BeautifulSoup(html, 'html.parser')
        return soup

    @staticmethod
    def books(ls):
        return SUtil.fnflatmap(SRedis.get_book, ls)


def main():
    ls = SRedis.links()
    b = SRedis.books(ls)
    print(len(b))


main()
