import brotli
import redis
import bs4
import requests
import sqlite3

limit = 1281
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
    conn = sqlite3.connect("item.db")
    c = conn.cursor()
    c.execute("""
            CREATE TABLE categories(
              id INTEGER PRIMARY KEY ASC AUTOINCREMENT UNIQUE NOT NULL,
              name VARCHAR(20) UNIQUE NOT NULL
            );
            """)

    c.execute("""
        CREATE TABLE ebooks(
          id INTEGER PRIMARY KEY ASC AUTOINCREMENT UNIQUE NOT NULL,
          title VARCHAR(255) NOT NULL,
          img VARCHAR(200),
          des VARCHAR(2000),
          isbn_10 VARCHAR(15),
          isbn_13 VARCHAR(20),
          format VARCHAR(10),
          author VARCHAR(30),
          pub_date DATE,
          size VARCHAR(15),
          pub VARCHAR(30),
          pages VARCHAR(30),
          host VARCHAR(30)
        );
        """)

    c.execute("""
        CREATE TABLE categories_data(
          id INTEGER PRIMARY KEY ASC AUTOINCREMENT UNIQUE NOT NULL,
          ebook_id INTEGER NOT NULL,
          cat_id INTEGER NOT NULL,
          FOREIGN KEY(ebook_id) REFERENCES ebooks(id),
          FOREIGN KEY(cat_id) REFERENCES categories(id)
        );
        """)

    conn.commit()
    for i in range(1, limit):
        for path in get_links(i):
            book = get_book(path)
            try:
                out = c.execute(
                    "INSERT INTO ebooks "
                    "(title, img, des, isbn_10, isbn_13, format, author, pub_date, size, pub, pages, host) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", [
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
                        book[4]
                    ])
                conn.commit()
                print(f'Inserted ({book[0]}, {book[1]}, ...) -> ebooks')

                ebook_id = out.lastrowid
                for cat in book[5]:
                    try:
                        c.execute("""INSERT INTO categories (name) VALUES (?)""", [cat])
                        print(f'Inserted ({cat}) -> categories')
                    except sqlite3.IntegrityError:
                        print(f'Skipping category {cat}')

                    c.execute("""SELECT id FROM categories WHERE name = ?""", [cat])
                    cat_id = c.fetchone()[0]
                    try:
                        c.execute("""INSERT INTO categories_data (ebook_id, cat_id) VALUES (?, ?)""",
                                  [ebook_id, cat_id])
                    except:
                        print('Failed to insert into categories_data table')
                        exit(0)
                    print(f'Inserted [{book[0]} {cat}] ({ebook_id}, {cat_id}) -> categories_data')

                conn.commit()
            except sqlite3.IntegrityError as err:
                print(err)
                continue

    c.close()
    conn.close()


main()
