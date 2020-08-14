import logging
import time
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime

#get own access to mongodb with own DB dcoument machine
client = MongoClient('')
db=client.twitter
user_index=db.user_index
auth_info=db.auth_tweets
tweet_info=db.tweet_info
retweet_info=db.retweet_info

#get own access to postgres with own RDS machine
engine=create_engine('')

user_index_query='''CREATE TABLE IF NOT EXISTS user_index(tweepy_time TIMESTAMP, user_id VARCHAR, user_name TEXT, user_screen_name TEXT);'''

auth_info_query='''CREATE TABLE IF NOT EXISTS auth_info(tweepy_time TIMESTAMP, tweet_id VARCHAR, auth_id VARCHAR, auth_image TEXT, follower_count INTEGER);'''

tweet_info_query='''CREATE TABLE IF NOT EXISTS tweet_info(tweepy_time TIMESTAMP, tweet_id VARCHAR, auth_id VARCHAR, language TEXT, tweet_text TEXT, hashtags TEXT, user_mentioned_id VARCHAR, reply_to_user_id VARCHAR, user_location TEXT);'''

retweet_info_query='''CREATE TABLE IF NOT EXISTS retweet_info(tweepy_time TIMESTAMP, tweet_id VARCHAR, auth_id VARCHAR, retweet_count INTEGER, favor_count INTEGER, retweeter_user_id VARCHAR, retweeter_follower_count INTEGER, retweeter_favor_count INTEGER);'''

engine.execute(user_index_query)
engine.execute(auth_info_query)
engine.execute(tweet_info_query)
engine.execute(retweet_info_query)

logging.critical('---------Setup Postgres Tables-----------')

def extraction(time1, time2):
    user_index_dict=list(user_index.find({"tweepy time":{'$gte':time1, '$lte':time2}}))
    auth_dict=list(auth_info.find({"tweepy time":{'$gte':time1, '$lte':time2}}))
    tweet_dict=list(tweet_info.find({"tweepy time":{'$gte':time1, '$lte':time2}}))
    retweet_dict=list(retweet_info.find({"tweepy time":{'$gte':time1, '$lte':time2}}))
    logging.critical('-------------Extracted Data from Mongo-------------')
    return user_index_dict, auth_dict, tweet_dict, retweet_dict

def mongo2list(user_index_dict, auth_dict, tweet_dict, retweet_dict):
    user_index_list=[]
    auth_list=[]
    tweet_list=[]
    retweet_list=[]
    for user, auth, tweet, retweet in zip(user_index_dict, auth_dict, tweet_dict, retweet_dict):
        user_index_list.append(user)
        auth_list.append(auth)
        tweet_list.append(tweet)
        retweet_list.append(retweet)
    logging.critical('-----------Converted Data to Lists-------------------')
    return user_index_list, auth_list, tweet_list, retweet_list

def load2postgres(user_index_list, auth_list, tweet_list, retweet_list):
    for user, auth, tweet, retweet in zip(user_index_list, auth_list, tweet_list, retweet_list):
        user_index_insert_query=f'''INSERT INTO user_index VALUES(%s,%s,%s,%s);'''
        user_index_data=[user['tweepy time'], str(user['user id']), user['user name'], user['user screen name']]
        engine.execute(user_index_insert_query, user_index_data)
        logging.critical('------------Added Data to User_index Postgres Table------------')

        auth_insert_query=f'''INSERT INTO auth_info VALUES(%s,%s,%s,%s,%s);'''
        auth_data=[auth['tweepy time'], str(auth['tweet id']), str(auth['author id']), auth['author image'], auth['follower count']]
        engine.execute(auth_insert_query, auth_data)
        logging.critical('------------Added Data to Auth_Index Postgres Table------------')

        tweet_insert_query=f'''INSERT INTO tweet_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        tweet_data=[tweet['tweepy time'], str(tweet['tweet id']), str(tweet['auth id']), tweet['language'], tweet['tweet text'], tweet['hashtags'], str(tweet['user mentioned id']), str(tweet['reply to user id']), tweet['user location']]
        engine.execute(tweet_insert_query, tweet_data)
        logging.critical('------------Added Data to Tweet_info Postgres Table------------')

        retweet_insert_query=f'''INSERT INTO retweet_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s);'''
        retweet_data=[retweet['tweepy time'], str(retweet['tweet id']), str(retweet['auth id']), retweet['retweet count'], retweet['favor count'], str(retweet['retweeter user id']), retweet['retweeter follower count'], retweet['retweeter favor count']]
        engine.execute(retweet_insert_query, retweet_data)
        logging.critical('------------Added Data to Retweet_info Postgres Table------------')


while True:
    time1=datetime.utcnow()
    time.sleep(60)
    time2=datetime.utcnow()
    user_index_dict, auth_dict, tweet_dict, retweet_dict=extraction(time1, time2)
    user_index_list, auth_list, tweet_list, retweet_list=mongo2list(user_index_dict, auth_dict, tweet_dict, retweet_dict)
    load2postgres(user_index_list, auth_list, tweet_list, retweet_list)
