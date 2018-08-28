import threading
from queue import Queue
import urllib.request
import os
import shutil
import time
import random
import eod_rate as er

URL = 'https://k0st1an.com/test.file'
DOWNLOAD = '/tmp'
WORKERS = 3
QUEUE = Queue()


def get_name(url):
    try:
        return url[url.rindex('/') + 1:]
    except ValueError:
        print('Error: URL incorrect: %s' % url)


def get_name_free(dst_file):
    extend = 0

    if os.path.exists(dst_file):
        while True:
            extend += 1
            if not os.path.exists(dst_file + '.%s' % extend):
                dst_file += '.%s' % extend
                break

    return dst_file


def download(url):
    dst_file = get_name_free(os.path.join(DOWNLOAD, get_name(url)))
    uid = random.randrange(1000, 9999)

    print('%s | Start downloading:' % uid)
    print('%s |  from: %s' % (uid, url))
    print('%s |  to: %s' % (uid, dst_file))

    with urllib.request.urlopen(url) as resp, open(dst_file, 'wb') as out:
            shutil.copyfileobj(resp, out)

    print('%s | Finish!' % uid)
    print('-' * 50)


def worker():
    while True:
        if not QUEUE.empty():
            print('Get queue')
            # url = QUEUE.get()
            er.rate_ready_to_cal(er.create_symbol_list('2018-07-09','2018-07-15'))
            QUEUE.task_done()


def main():
    index = 0

    for w in range(WORKERS):
        t = threading.Thread(target=worker, name='worker-%s' % w)
        t.daemon = True
        t.start()

    # while True:
    #     if index == 10:
    #         time.sleep(2)
    #         QUEUE.put(URL)
    #     index += 1

    QUEUE.join()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()