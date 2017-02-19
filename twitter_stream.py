from models import Session, Tweet, TwitterUser, TweetUrl
import argparse
import tweepy
import psycopg2
import os
import sys
import csv

CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

OAUTH_TOKEN = os.environ["TWITTER_OAUTH_TOKEN"]
OAUTH_TOKEN_SECRET = os.environ["TWITTER_OAUTH_TOKEN_SECRET"]


def add_tweet(status):
    if hasattr(status, 'retweeted_status'):
        add_tweet(status.retweeted_status)

    user_data = {
        'id': status.author.id,
        'created_at': status.author.created_at,

        'favourites_count': status.author.favourites_count,
        'followers_count': status.author.followers_count,
        'friends_count': status.author.friends_count,

        'name': status.author.name,
        'screen_name': status.author.screen_name,
        'description': status.author.description,
        'statuses_count': status.author.statuses_count,
        'listed_count': status.author.listed_count,

        'time_zone': status.author.time_zone,
        'utc_offset': status.author.utc_offset
    }
    user = TwitterUser(**user_data)

    user_in_db = Session.query(TwitterUser).filter(TwitterUser.id == status.author.id).first()
    if user_in_db:
        user = Session.merge(user, load=True)
    else:
        Session.add(user)

    urls = []
    for url_status in status.entities['urls']:
        url_in_db = Session.query(TweetUrl).filter(
            TweetUrl.url == url_status['url'],
            TweetUrl.indice_start == url_status['indices'][0],
            TweetUrl.tweet_id == status.id
        ).first()
        if not url_in_db:
            url_in_db = TweetUrl(
                url=url_status['url'],
                indice_start=url_status['indices'][0],
                indice_end=url_status['indices'][1],
                display_url=url_status.get('display_url', None),
                expanded_url=url_status['expanded_url']
            )
            # url_in_db = update_tweet_url(url_in_db)
        urls.append(url_in_db)

    tweet_data = {
        'id': status.id,
        'created_at': status.created_at,
        'author_id': status.author.id,

        'hashtags': status.entities['hashtags'],
        'urls_jsonb': status.entities['urls'],
        'media': status.entities.get('media', None),  # To debug ?
        'urls': urls,
        'user_mentions': status.entities['user_mentions'],

        'favorited': status.favorited,
        'favorite_count': status.favorite_count,
        'is_quote_status': status.is_quote_status,
        'lang': status.lang,
        # '_metadata': status.metadata,
        'possibly_sensitive': status.possibly_sensitive if hasattr(status, 'possibly_sensitive') else None,

        'retweeted': status.retweeted,
        'retweet_count': status.retweet_count,
        'is_retweet': True if status.text[:3] == "RT " else False,
        'original_tweet_id': status.retweeted_status.id if hasattr(status, 'retweeted_status') else None,

        'source': status.source,
        'text': status.text,
        'truncated': status.truncated
    }
    tweet_in_db = Session.query(Tweet).filter(Tweet.id == status.id).first()
    if tweet_in_db:
        try:
            print("Committing (update): {}".format(status.id))
            for col in ['favorite_count', 'retweeted', 'retweet_count', 'favorited']:
                setattr(tweet_in_db, col, tweet_data[col])
            Session.commit()
        except psycopg2.IntegrityError as e:
            print('In Session...')
            print(e)
    else:
        try:
            print("Committing (add): {}".format(status.id))
            tweet = Tweet(**tweet_data)
            Session.add(tweet)
            Session.commit()
        except psycopg2.IntegrityError as e:
            print('In add...')
            print(e)


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

    def on_error(self, code):
        ''' Handle errors originating from the stream'''
        if code == 420:
            print('Error 420: Rate limits')
        else:
            print('Error: ', code)
        return True  # don't kill the stream

    def on_timeout(self):
        ''' Handle timeouts from the Twitter API '''
        print('Time out...')
        return True  # don't kill the stream

    def send_error(self):
        pass
        # fromaddr = "twitterpipeline@yahoo.com"
        # toaddr = self.email
        # msg = """ Hey! There was an error with your twitter stream pipeline. It's on you to check it out ands see if we need a restart.
        #           Traceback:
        #           %s """ % (traceback.format_exc())
        # server = SMTP_SSL('smtp.mail.yahoo.com:465')
        # server.login("twitterpipeline@yahoo.com", "Sk8board")
        # server.sendmail(fromaddr, toaddr, msg)
        # server.quit()


def read_keywords(filename):
    ''' Reads in keywords from txt to a list '''

    file = open(filename)
    reader = csv.reader(file)

    keywords = [row[0] for row in reader]
    print('Track:', keywords)
    return keywords


if __name__ == "__main__":

    # keyword and email configuration
    opt_parser = argparse.ArgumentParser()
    opt_parser.add_argument("-k", "--keywords", help="Path to your keywords file", required=True)
    opt_parser.add_argument("-e", "--email", help="Email address for error notification", required=False)
    args = opt_parser.parse_args()

    # initialize auth using tweepy's built in oauth handling
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    # create our listener and stream
    listener = TwitterStreamListener(args.email)
    stream = tweepy.streaming.Stream(auth, listener)

    # define terms we want to filter on
    query_terms = read_keywords(args.keywords)

    # filter the stream on query_terms
    stream.filter(track=query_terms)
