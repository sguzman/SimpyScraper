import requests
import bs4
import redis

base = "http://23.95.221.108/page/"
redisHash = 'ebooks'

red = redis.StrictRedis(host='localhost', port=6379, db=0)
hashmap = red.hgetall(redisHash)

limit = 1263


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_url(text):
    return remove_prefix(text, 'https://it-eb.com')


def fnmap(func, collection):
    new_list = []
    for item in collection:
        new_list.append(func(item))

    return new_list


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

    return fnmap(remove_url, fnmap(lambda x: x.find('a').attrs['href'], arts))


def get_book(path):
    url = base + path

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    return soup


def main():
    for i in range(1, limit + 1):
        print(get_links(i))


main()
