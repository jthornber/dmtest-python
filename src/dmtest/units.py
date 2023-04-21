SECTOR_SIZE = 512


def kilo(n):
    return n * 2


def meg(n):
    return 1024 * kilo(n)


def gig(n):
    return 1024 * meg(n)
