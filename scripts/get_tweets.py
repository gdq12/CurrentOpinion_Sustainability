import config
import time
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import logging
from pymongo import MongoClient
from datetime import datetime
import sys

#get own access to postgres with own RDS machine
client = MongoClient('')
db=client.twitter
user_index=db.user_index
auth_tweets=db.auth_tweets
tweet_info=db.tweet_info
retweet_info=db.retweet_info


def authenticate():
    """
    To create authentication token for twitter, the necessary credentials should be in the config.py file
    """
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

class TwitterListener(StreamListener):

    def on_data(self, data):

        t = json.loads(data)

        #author tweet table
        try:
            tweet_id=t['id']
        except KeyError:
            tweet_id=0
        try:
            auth_id=t['user']['id']
        except KeyError:
            auth_id=0
        try:
            user_id=t['user']['id']
            user_name=t['user']['name']
            user_screen_name=t['user']['screen_name']
            tweet4={'tweepy time':datetime.utcnow(), 'user id':user_id, 'user name':user_name, 'user screen name':user_screen_name}
            user_index.insert_many([tweet4])
            logging.critical('---------Added to User_index Mongo Collection-----------')
        except KeyError:
            pass
        try:
            auth_image=t['user']['profile_image_url_https'].replace('_normal', '')
        except KeyError:
            auth_image='Not provided'
        try:
            followers_count=t['user']['followers_count']
        except KeyError:
            followers_count=0
        tweet1 = {'tweepy time':datetime.utcnow(), 'tweet id':tweet_id, 'author id':auth_id, 'author image':auth_image, 'follower count':followers_count}
        auth_tweets.insert_many([tweet1])
        logging.critical('---------Added to Auth_tweets Mongo Collection-----------')

        #tweet info table
        try:
            language=t['lang']
        except KeyError:
            language='Not provided'
        if 'extended_tweet' in t:
            text=t['extended_tweet']['full_text']
        else:
            text=t['text']
        hashtags=[]
        if 'extended_tweet' in t:
            for hashtag in t['extended_tweet']['entities']['hashtags']:
                hashtags.append(hashtag['text'])
        elif 'hashtags' in t['entities'] and len(t['entities']['hashtags']) > 0:
            hashtags=[item['text'] for item in t['entities']['hashtags']]
        user_mention_id=[]
        for i in range(len(t['entities']['user_mentions'])):
            try:
                user_mention_id.append(t['entities']['user_mentions'][i]['id'])
            except KeyError:
                user_mention_id.append(0)
            try:
                user_id=t['entities']['user_mentions'][i]['id']
                user_name=t['entities']['user_mentions'][i]['name']
                user_screen_name=t['entities']['user_mentions'][i]['screen_name']
                tweet4={'tweepy time':datetime.utcnow(), 'user id':user_id, 'user name':user_name, 'user screen name':user_screen_name}
                user_index.insert_many([tweet4])
                logging.critical('---------Added to User_index Mongo Collection-----------')
            except KeyError:
                pass
        try:
            reply_to_user_id=t['in_reply_to_user_id']
        except KeyError:
            reply_to_user_id=0
        try:
            user_id=t['in_reply_to_user_id']
            user_name='None'
            user_screen_name=t['in_reply_to_screen_name']
            tweet4={'tweepy time':datetime.utcnow(), 'user id':user_id, 'user name':user_name, 'user screen name':user_screen_name}
            user_index.insert_many([tweet4])
            logging.critical('---------Added to User_index Mongo Collection-----------')
        except KeyError:
            pass
        try:
            reply_to_status_id=t['in_reply_to_status_id']
        except KeyError:
            reply_to_status_id=0
        try:
            self_loc=t['user']['location']
        except KeyError:
            self_loc='None'
        tweet2={'tweepy time':datetime.utcnow(), 'tweet id':tweet_id, 'auth id':auth_id, 'language':language, 'tweet text':text, 'hashtags':hashtags, 'user mentioned id': user_mention_id, 'reply to user id':reply_to_user_id, 'user location':self_loc}
        tweet_info.insert_many([tweet2])
        logging.critical('---------Added to Tweet_info Mongo Collection-----------')

        #retweet info table
        try:
            retweet_count=t['retweeted_status']['retweet_count']
        except KeyError:
            retweet_count=0
        try:
            favor_count=t['retweeted_status']['favorite_count']
        except KeyError:
            favor_count=0
        try:
            retweeter_user_id=t['retweeted_status']['user']['id']
        except KeyError:
            retweeter_user_id=0
        try:
            user_id=t['retweeted_status']['user']['id']
            user_name=t['retweeted_status']['user']['name']
            user_screen_name=t['retweeted_status']['user']['screen_name']
            tweet4={'tweepy time':datetime.utcnow(), 'user id':user_id, 'user name':user_name, 'user screen name':user_screen_name}
            user_index.insert_many([tweet4])
            logging.critical('---------Added to User_index Mongo Collection-----------')
        except KeyError:
            pass
        try:
            retweeter_follower_count=t['retweeted_status']['user']['followers_count']
        except KeyError:
            retweeter_follower_count=0
        try:
            retweeter_favor_count=t['retweeted_status']['user']['favorites_count']
        except KeyError:
            retweeter_favor_count=0
        tweet3={'tweepy time':datetime.utcnow(), 'tweet id':tweet_id, 'auth id':auth_id, 'retweet count':retweet_count, 'favor count':favor_count, 'retweeter user id':retweeter_user_id, 'retweeter follower count':retweeter_follower_count, 'retweeter favor count':retweeter_favor_count}
        retweet_info.insert_many([tweet3])
        logging.critical('---------Added to Retweet_info Mongo Collection-----------')
        tweet4={'tweepy time':datetime.utcnow(), 'user id':user_id, 'user name':user_name, 'user screen name':user_screen_name}
        user_index.insert_many([tweet4])
        logging.critical('---------Added to User_index Mongo Collection-----------')

    def on_error(self, status):
        if status == 420:
            print(status)
            return False

    def on_limit(self, status):
        print('Rate Limit Exceeded, sleep for 6 minutes')
        time.sleep(6*60)
        return True

if __name__ == '__main__':

    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    while True:
        try:
            stream.filter(track=['sustainability', 'climatechange', 'recylce', 'circulareconomy'], stall_warnings=True)
        except:
            logging.critical('------------Problem with streamer, reinitiate in 10 seconds--------------')
            time.sleep(10)
            continue
