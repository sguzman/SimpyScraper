import sys

for line in sys.path:
     print(line)
import brotli
import bs4
import redis
import requests
from scrape import items_pb2

limit = 1283
red = redis.StrictRedis()
hash_map = red.hgetall('ebooks')


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
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

    html = get_redis(url)

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.find('h1', class_='post-title').get_text()
    e_id = soup.find('input', {'type': 'hidden', 'name': 'comment_post_ID'})['value']
    host = get_redis(f'http://23.95.221.108/download.php?id={e_id}')

    categories = [x.get_text() for x in soup.find('p', class_='post-btm-cats').findAll('a')]

    details = soup.find('div', class_='book-details').ul
    detail_keys = [x.get_text() for x in details.findAll('span')]
    detail_raw_vals = [x.get_text() for x in details.findAll('li')]
    detail_dict = {k.rstrip(':').lower(): remove_prefix(v, k) for (k, v) in zip(detail_keys, detail_raw_vals)}

    return [arts,
            soup.find('div', class_='book-cover').img['src'],
            detail_dict,
            soup.find('div', class_='entry-inner').get_text(),
            host,
            categories
            ]


# {'isbn-13', 'authors', 'format', 'publication date', 'size', 'publisher', 'isbn-10', 'pages'}
def main():
    some_file = open("items.txt", "wb")
    for i in range(1, limit + 1):
        for path in get_links(i):
            book = get_book(path)
            print(book)
            b = items_pb2.Book()

            b.title = book[0]
            b.img = book[1]
            b.desc = book[3]
            b.host = book[4]

            b.categories.extend(book[5])

            if book[2].get('isbn-10') is not None:
                b.isbn_10 = book[2]['isbn-10']
            if book[2].get('isbn-13') is not None:
                b.isbn_13 = book[2]['isbn-13']
            if book[2].get('authors') is not None:
                b.authors = book[2]['authors']
            if book[2].get('format') is not None:
                b.format = book[2]['format']
            if book[2].get('publication date') is not None:
                b.pub_date = book[2]['publication date']
            if book[2].get('size') is not None:
                b.size = book[2]['size']
            if book[2].get('publisher') is not None:
                b.pub = book[2]['publisher']
            if book[2].get('pages') is not None:
                b.pages = book[2]['pages']

            some_file.write(b.SerializeToString())
        some_file.flush()
    some_file.close()


main()
