import redis
import bs4
import requests
from .globals import *
from .util import *


red = redis.StrictRedis()
hashmap = red.hgetall(redisHash)


def get_redis(url):
    key = str.encode(url)
    if hashmap.get(key) is None:
        print('Missed Http cache for url %s' % url)
        html = requests.get(url).text
        red.hset(redisHash, url, html)
    else:
        print('Hit Http cache for url %s' % url)
        html = hashmap.get(key)

    return html


def get_links(i):
    url = base + str(i)

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return [remove_url(x.find('a').attrs['href']) for x in arts]


def get_book(path):
    url = base + path

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    return soup


def links():
    return fnflatmap(get_links, range(1, limit + 1))


def books(link):
    return fnflatmap(get_book, link)
