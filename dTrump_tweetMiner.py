from pymongo import MongoClient
from requests_oauthlib import OAuth1
import cnfg
from TwitterSearch import *
import time
import sys

# Authentication details.
config = cnfg.load(".twitter_config")
oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])

# MongoDB Connection
client = MongoClient('##.##.###.##', port)
db = client.election2016
hillary_tweets = db.dTrump_tweets

try:
    tso = TwitterSearchOrder()
    tso.set_keywords(["donald trump", 'since:2015-06-16'])
    tso.set_language('en')

    ts = TwitterSearch(
        consumer_key = config["consumer_key"],
        consumer_secret = config["consumer_secret"],
        access_token = config["access_token"],
        access_token_secret = config["access_token_secret"]
     )

    def my_callback_closure(current_ts_instance): # accepts ONE argument: an instance of TwitterSearch
        queries, tweets_seen = current_ts_instance.get_statistics()
        if queries > 0 and (queries % 5) == 0: # trigger delay every 5th query
            time.sleep(60) # sleep for 60 seconds
 
    i=0
    for tweet in ts.search_tweets_iterable(tso, callback=my_callback_closure):
        hillary_tweets.insert_one({
                        'user_id': tweet['user']['id_str'],
                        'screen_name': tweet['user']['screen_name'],
                        'text': tweet['text'],
                        'created_at': tweet['created_at'],
                        'hashtags': [tag['text'] for tag in tweet['entities']['hashtags']],
                        'retweeted': tweet['retweeted'],
                        'retweet_count': tweet['retweet_count'],
                        'favorite_count': tweet['favorite_count'],
                        'mentions': [user['id_str'] for user in tweet['entities']['user_mentions']]
                        })
        sys.stdout.write('\r')
        sys.stdout.write("Mined Tweets: %d" % i)
        sys.stdout.flush()
        i+=1
    
except TwitterSearchException as e:
    print(e)