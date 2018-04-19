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


def removeUrl(text):
    return remove_prefix(text, 'https://it-eb.com')


def fnmap(func, collection):
    newList = []
    for item in collection:
        newList.append(func(item))

    return newList


def getRedis(url):
    html = ''
    key = str.encode(url)
    if hashmap.get(key) is None:
        print('Missed Http cache for url %s' % url)
        html = requests.get(url).text
        red.hset(redisHash, url, html)
    else:
        print('Hit Http cache for url %s' % url)
        html = hashmap.get(key)

    return html


def getLinks(i):
    url = base + str(i)

    html = getRedis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')

    return fnmap(removeUrl, fnmap(lambda x: x.find('a').attrs['href'], arts))


def getBook(path):
    url = base + path

    html = getRedis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    return soup


def main():
    for i in range(1, limit + 1):
        print(getLinks(i))


main()
