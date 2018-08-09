# encoding: utf-8
from pdb import set_trace
import random
import time
import numpy
from threading import Thread
from urllib import urlencode, quote

import random
import requests

class QueryTimingThread(Thread):

    def __init__(self, urls):
        Thread.__init__(self)
        self.urls = urls

    def run(self):
        self.elapsed = []
        self.exceptions = []
        for url in self.urls:
            a = time.time()
            exception = self.do_query(url)
            self.elapsed.append(time.time()-a)
            if exception:
                self.exceptions.append((url, exception))

    def do_query(self, url):
        print url
        try:
            response = requests.get(url)
            return None
        except Exception, e:
            return e

    def report(self):
        print ""
        print "Timing results for %s" % self.urls[0]
        print "------------------"
        # print "Total time elapsed: %s" % numpy.sum(self.elapsed)
        print "Mean time elapsed: %.2f" % numpy.mean(self.elapsed)
        print "Median time elapsed: %.2f" % numpy.median(self.elapsed)
        m = numpy.argmax(self.elapsed)
        print "Max time elapsed: %.2f" % self.elapsed[m]
        print "Max url: %s" % self.urls[m]
        print "Raw data:"
        for i, url in enumerate(self.urls):
            print "(%.2f) %s" % (self.elapsed[i], url)
        for (url, e) in self.exceptions:
            print "Exception: %s: %s" % (url, e)
        print ""

size = 50
pages = 10
thread_count = 10
base_url = "http://qa.circulation.librarysimplified.org"

queries = [
    {
        'language': 'eng',
        'category': 'Adult Fiction',
        'params': {
            'order': 'author',
            'available': 'now',
            'collection': 'full'
        }
    },
    {
        'language': 'eng',
        'category': 'Adult Fiction',
        'params': {
            'order': 'title',
            'available': 'all',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Adult Nonfiction',
        'params': {
            'order': 'author',
            'available': 'now',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Adult Nonfiction',
        'params': {
            'order': 'title',
            'available': 'all',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'English Best Sellers',
        'params': {
            'order': 'author',
            'available': 'all',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Young Adult Fiction',
        'params': {
            'order': 'added',
            'available': 'all',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Children and Middle Grade',
        'params': {
            'order': 'author',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Adventure',
        'params': {
            'order': 'author',
            'available': 'main',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Classics',
        'params': {
            'order': 'title',
            'available': 'now',
            'collection': 'full'
        }
    },
    {
        'language': 'eng',
        'category': 'Police Procedural',
        'params': {
            'order': 'title',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Biography & Memoir',
        'params': {
            'order': 'author',
            'available': 'always',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Business',
        'params': {
            'order': 'added',
            'available': 'now',
            'collection': 'full'
        }
    },
    {
        'language': 'eng',
        'category': 'Parenting & Family',
        'params': {
            'order': 'author',
            'available': 'all',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Cooking',
        'params': {
            'order': 'title',
            'available': 'all',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Latin American History',
        'params': {
            'order': 'author',
            'available': 'all',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Pets',
        'params': {
            'order': 'title',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Photography',
        'params': {
            'order': 'author',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Music',
        'params': {
            'order': 'added',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Life Strategies',
        'params': {
            'order': 'title',
            'available': 'all',
            'collection': 'main'
        }
    },
    {
        'language': 'eng',
        'category': 'Buddhism',
        'params': {
            'order': 'author',
            'available': 'all',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Computers',
        'params': {
            'order': 'added',
            'available': 'now',
            'collection': 'featured'
        }
    },
    {
        'language': 'eng',
        'category': 'Self Help',
        'params': {
            'order': 'author',
            'available': 'all',
            'collection': 'full'
        }
    },
    {
        'language': 'eng',
        'category': 'True Crime',
        'params': {
            'order': 'title',
            'available': 'all',
            'collection': 'full'
        }
    }
]

def urls_from_query(query, pages, size):
    urls = []
    for i in range(pages):
        if i > 0:
            query['params']['after'] = i * size
        url = quote("%s/feed/%s/%s?%s" % (
            base_url, query['language'], query['category'], urlencode(query['params'])), safe=':/?=&')
        urls.append(url)
    return urls

threads = [QueryTimingThread(urls=urls_from_query(random.choice(queries), pages, size)) for i in range(thread_count)]

for t in threads:
    t.start()
for t in threads:
    t.join()
for t in threads:
    t.report()