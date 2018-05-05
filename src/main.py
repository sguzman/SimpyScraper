import brotli
import redis
import bs4
import requests
import sqlite3

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
            soup.find('div', class_='book-cover').img['src'],
            detail_dict,
            soup.find('div', class_='entry-inner').get_text()
            ]


# {'isbn-13', 'authors', 'format', 'publication date', 'size', 'publisher', 'isbn-10', 'pages'}
def main():
    conn = sqlite3.connect("item.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE ebooks(
          id INTEGER PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE,
          title VARCHAR(255) UNIQUE,
          img VARCHAR(200) UNIQUE,
          des VARCHAR(2000),
          isbn_10 VARCHAR(15),
          isbn_13 VARCHAR(20),
          format VARCHAR(10),
          author VARCHAR(30),
          pub_date DATE,
          size VARCHAR(15),
          pub VARCHAR(30),
          pages VARCHAR(30)
        );
        """)
    conn.commit()
    for i in range(1, limit):
        for path in get_links(i):
            book = get_book(path)
            try:
                c.execute(
                    "INSERT INTO ebooks (title, img, des, isbn_10, isbn_13, format, author, pub_date, size, pub, pages) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", [
                        book[0],
                        book[1],
                        book[3],
                        book[2].get('isbn-10'),
                        book[2].get('isbn-13'),
                        book[2].get('format'),
                        book[2].get('authors'),
                        book[2].get('publication date'),
                        book[2].get('size'),
                        book[2].get('publisher'),
                        book[2].get('pages'),
                    ])
            except sqlite3.IntegrityError:
                print(f'Skipping non-unique title, {book[0]}')
                continue

        conn.commit()

    c.close()
    conn.close()


main()
