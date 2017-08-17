import argparse
import tweepy
import sys
import os
from raven import Client
from models import Tweet, add_tweet
from sqlalchemy import desc
import sqlalchemy

CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")

OAUTH_TOKEN = os.environ.get("TWITTER_OAUTH_TOKEN")
OAUTH_TOKEN_SECRET = os.environ.get("TWITTER_OAUTH_TOKEN_SECRET")

SENTRY_CLIENT = os.environ["SENTRY_CLIENT"]
client = Client(SENTRY_CLIENT)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

API = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

if (not API):
    print("Can't Authenticate")
    sys.exit(-1)


def twitter_search(since_id, search_query):
    error_count = 0
    max_id = None
    print('Query: \n\t%s' % search_query)
    print('Last id: \n\t%s' % since_id)
    while True:
        try:
            new_tweets = API.search(q=search_query, count=100,
                                    max_id=max_id,
                                    since_id=str(since_id))
        except tweepy.TweepError as e:
            # Just exit if any error
            error_count += 1
            print("some error : " + str(e))
            # client.captureMessage('on_error: {}'.format(e))
            client.captureException()
            if error_count > 2:
                break
            else:
                print('Retry...')
                continue
        print(len(new_tweets))
        if not new_tweets:
            print("No more tweets found")
            break
        for tweet in new_tweets:
            add_tweet(tweet)
        max_id = str(new_tweets[-1].id - 1)
    print("End")


url_list = [
    'tennis',
    'wta',
    'atp',
    'AusOpen',
    'AustralianOpen',
    'FrenchOpen',
    'RolandGarros',
    'UsOpen',
    'Wimbledon',
    'FedCup',
    'DavisCup',
    'HopmanCup',
]

# https://stackoverflow.com/questions/4976547/twitter-api-search-too-complex/7200201#7200201
assert len(url_list) < 23, 'Too many operators'

urls = ' OR '.join(url_list)

if __name__ == '__main__':
    search_query = urls
    since_id = None

    # keyword and email configuration
    opt_parser = argparse.ArgumentParser()
    # opt_parser.add_argument("-k", "--keywords", help="Path to your keywords file", required=True)
    opt_parser.add_argument("--stock", action='store_true', default=False)
    args = opt_parser.parse_args()
    if args.stock is False:
        try:
            since_id = Tweet.query().order_by(desc(Tweet.created_at)).limit(1).one().id  # the get last twitter ID
        except sqlalchemy.orm.exc.NoResultFound:
            pass
    twitter_search(since_id, search_query)
