import bs4
import requests
import os
import brotli

limit = 1290


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):].replace('/', '')
    return text


def get_links(i):
    url = f"http://23.95.221.108/page/{i}"
    html = requests.get(url).text

    soup = bs4.BeautifulSoup(html, 'html.parser')
    arts = soup.findAll('article')
    hrefs = [x.find('a').attrs['href'] for x in arts]

    return [remove_prefix(x, 'https://it-eb.com/') for x in hrefs]


def get_book(path):
    url = f"http://23.95.221.108/{path}"
    return requests.get(url).text


def write(file_name, content):
    fd = open(file_name, 'wb')
    fd.write(content)
    fd.flush()
    fd.close()


def main():
    dir_name = "../txt/"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    for i in range(1, limit + 1):
        for path in get_links(i):
            file = f'{dir_name}{path}.txt.brotli'
            if not os.path.exists(file):
                print(file)
                html = get_book(path)
                bro = brotli.compress(html.encode(), brotli.MODE_TEXT)
                write(file, bro)


main()
