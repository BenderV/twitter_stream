from models import Session, add_tweet
import argparse
import tweepy
import os
import sys
import utils

CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

OAUTH_TOKEN = os.environ["TWITTER_OAUTH_TOKEN"]
OAUTH_TOKEN_SECRET = os.environ["TWITTER_OAUTH_TOKEN_SECRET"]


class TwitterStreamListener(tweepy.StreamListener):

    def __init__(self, email):
        tweepy.StreamListener.__init__(self)
        self.email = email

    def on_status(self, status):
        ''' Stores incoming tweets into tweets.db '''

        # print("Incoming tweet...")
        # print(status.text)
        #
        # if not status.entities['urls']:
        #    return

        # if any([url for url in status.entities['urls'] if 'arxiv.org' in url['expanded_url']]):
        #    pass
        # else:
        #    return

        try:
            add_tweet(status)
            print(status.text if status.text else "")
        except KeyboardInterrupt as e:
            Session.rollback()
            print(e)
            sys.exit(0)
            return
        except Exception as e:
            print(sys.stderr, 'Encountered Exception: ', e)
            Session.rollback()
            return


if __name__ == "__main__":

    # keyword and email configuration
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-k", "--keywords", help="Path to your keywords file", required=True)
    args = argparser.parse_args()

    # initialize auth using tweepy's built in oauth handling
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    # create our stream
    stream = tweepy.streaming.Stream(auth)

    # define terms we want to filter on
    query_terms = utils.read_keywords(args.keywords)

    # filter the stream on query_terms
    stream.filter(track=query_terms)
