from .redis import links
from .redis import books


def main():
    ls = links()
    b = books(ls)
    print(len(b))


main()
