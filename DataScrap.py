"""
Scrapping data from twitter every 5 minute then saving to buffer and inserting to Database!
it's designed for Raspberry pi devices, because data streams reading/writing from buffer not from file!
"""

from http.client import IncompleteRead
import json
import logging
import warnings
import time
import nltk
import numpy
import pymysql.cursors
import threading
import os
from collections import Counter
from tweepy import OAuthHandler, API
from tweepy import Stream
from tweepy.streaming import StreamListener

# trying to import StringIO
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


# list of words which want to capture from twitts
my_dict2 = {'cake': 1, 'happy': 1, 'healthy': 1, 'amazing': 1, 'beautiful': 1}
sequence = []
warnings.filterwarnings("ignore")
# Twitter api keys
access_token = "YOUR-KEY"
access_token_secret = "YOUR-KEY"
consumer_key = "YOUR-KEY"
consumer_secret = "YOUR-KEY"
# Connecting to Twitter
auth_handler = OAuthHandler(consumer_key, consumer_secret)
auth_handler.set_access_token(access_token, access_token_secret)
twitter_client = API(auth_handler)

# Variables
t_end = 0                   # reset time
buffer = StringIO()         # buffer
start_time = time.time()    # get start time
x = []                      # array to saving captured words

# Inserting to Database - Auto insert every 5 minute to data bae ( mysql)


def insert_db():

    time_now = time.localtime()
    date_in = time.strftime("%Y-%m-%d %H:%M:%S", time_now)

    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 db='pyCount',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # before use it create Database name "pyCount"
            sql = "CREATE TABLE if not EXISTS `wordCount` (`id` int(11) NOT NULL AUTO_INCREMENT," \
                  "`words` varchar(255) COLLATE utf8_bin NOT NULL," \
                  "`count` int(12) COLLATE utf8_bin NOT NULL," \
                  "`create_at` DATETIME NOT NULL ," \
                  "PRIMARY KEY (`id`))" \
                  " ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin " \
                  "AUTO_INCREMENT=1 ;"
            cursor.execute(sql)
            # Create a new record
            for key in x[-1]:
                sql = "INSERT INTO `wordCount` (`words`, `count`,`create_at`) VALUES (%s, %s,%s)"
                cursor.execute(sql, (key[0], key[1], date_in))
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    finally:
        connection.close()


# Reading data from buffer


def read_buff():
    content = ""
    # set buffer point to first of buffer address
    buffer.seek(0)
    # reading buffer contents
    content = buffer.getvalue()
    # reading line by line json data
    for line in content.splitlines():
        m = json.loads(line)
        # converting to lower case
        extracted = m['text'].lower()
        # searching for words which is exist in our main list
        tokenized = nltk.word_tokenize(extracted)
        for word in tokenized:
            nplist = []
            # Counting frequency every words and inserting in array
            if my_dict2.get(word) is not None:
                sequence.append(word)
                nplist.append(sequence)
                A = numpy.array(nplist)
                b = Counter(A.flat)
                z = b.most_common(5)
                x.append(z)
                b.clear()
    # deleting all array to re-use in next run
    del sequence[:]
    del nplist[:]
    A = numpy.delete(A,0,0)

# listening to stream for every 5 minute then re-running program


class PyStreamListener(StreamListener):
    def __init__(self):
        self.timeRun = time.time() + (60 * 0.5)
        super(PyStreamListener, self).__init__()

    # scarping data from twitter and write into buffer

    def on_data(self, data):

        while time.time() < self.timeRun:
            try:
                # writing to buffer
                buffer.writelines(data)
                return True

            except IncompleteRead:
                continue

            except Exception as ex:
                logging.error(ex)
            return True

        if time.time() > self.timeRun:
            return False

    def on_error(self, status):
        print(status)

    def on_status(self, status):
        if time.time() > self.timeRun:
            return False


def start_main():
    # get time to controlling running program
    going_time = time.localtime()
    threading.Timer(300, start_main).start()
    # setting data parameter to twitter
    listener = PyStreamListener()
    stream = Stream(auth_handler, listener, )
    stream.filter(track=['birthday'])
    # reading data after scrapping
    read_buff()
    # inserting data to database after analysing
    insert_db()
    print("The new twitts scraped at:", time.strftime("%Y-%m-%d %H:%M:%S", going_time))
    buffer.seek(0, os.SEEK_END)
    print("Buffer used: ", buffer.tell())
    print("Data Scraped: ", x[-1])
    print("Starting next capture ... ")
    # resetting buffer and array to re-use again
    del x[:]
    buffer.truncate(0)
    buffer.seek(0)

# starting main program
start_main()
