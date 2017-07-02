from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import (Column,
                        Integer,
                        BigInteger,
                        String,
                        Boolean,
                        DateTime,
                        ForeignKey
                        )
from datetime import datetime
import psycopg2
import os
import sys

# set up our local DB to store the tweets
host = os.environ['DATABASE_HOST']
database = os.environ['DATABASE_NAME']
user = os.environ['DATABASE_LOGIN']
password = os.environ['DATABASE_PASSWORD']

print(host, database)
db = create_engine('postgresql://' + user + ':' + password + '@' + host + '/' + database)
Base = declarative_base(bind=db)
Base.metadata.schema = 'twitter'
Session = scoped_session(sessionmaker(db))
# db.echo = True


class AuditMixin(object):
    audit_created_at = Column(DateTime, default=datetime.utcnow)
    audit_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def query(cls):
        return Session.query(cls)

    @classmethod
    def get_by(cls, **kw):
        return cls.query().filter_by(**kw).first()

    @classmethod
    def get_or_create(cls, **kw):
        r = cls.get_by(**kw)
        if not r:
            r = cls(**kw)
            Session.add(r)
        return r


class Tweet(AuditMixin, Base):
    ''' Class to store individual tweet traits '''

    __tablename__ = 'tweets'

    id = Column(BigInteger, primary_key=True)
    author_id = Column(BigInteger, ForeignKey('twitter_users.id'))
    author = relationship("TwitterUser", back_populates="tweets")

    created_at = Column(DateTime)
    favorited = Column(Boolean)
    coordinates = Column(JSONB)

    # Entities
    hashtags = Column(JSONB)
    media = Column(JSONB)
    urls_jsonb = Column(JSONB)

    urls = relationship("TweetUrl", back_populates="tweet")

    user_mentions = Column(JSONB)

    favorite_count = Column(Integer)
    is_quote_status = Column(Boolean)
    lang = Column(String)
    _metadata = Column(JSONB)
    place = Column(JSONB)
    possibly_sensitive = Column(Boolean)

    retweeted = Column(Boolean)
    retweet_count = Column(Integer)
    is_retweet = Column(Boolean)
    original_tweet_id = Column(BigInteger)

    source = Column(String)
    text = Column(String)
    truncated = Column(Boolean)


class Url(AuditMixin, Base):
    __tablename__ = 'urls'

    url = Column(String, primary_key=True)
    parsing_info = Column(JSONB)
    original_url = Column(String)
    original_url_netloc = Column(String)


class TweetUrl(AuditMixin, Base):
    __tablename__ = 'tweet_urls'

    id = Column(Integer, autoincrement=True, primary_key=True)
    tweet_id = Column(BigInteger, ForeignKey('tweets.id'))  # , primary_key=True)
    tweet = relationship("Tweet", back_populates="urls")

    url = Column(String)
    indice_start = Column(Integer)
    indice_end = Column(Integer)
    display_url = Column(String)
    expanded_url = Column(String, ForeignKey('urls.url'))

    real_url = Column(String)
    real_url_netloc = Column(String)
    real_url_path = Column(String)


class TwitterUser(AuditMixin, Base):
    ''' Class to store individual user traits '''

    __tablename__ = 'twitter_users'

    id = Column(BigInteger, primary_key=True)
    created_at = Column(DateTime)

    favourites_count = Column(Integer)
    followers_count = Column(Integer)
    friends_count = Column(Integer)

    name = Column(String)
    screen_name = Column(String)
    description = Column(String)
    statuses_count = Column(Integer)
    listed_count = Column(Integer)

    time_zone = Column(String)
    utc_offset = Column(Integer)
    tweets = relationship("Tweet", back_populates="author")


def add_tweet(status):
    try:
        print(status.text if status.text else "")

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
            url_in_db = Session.query(Url).filter(Url.url == url_status['expanded_url']).first()
            if not url_in_db:
                url_in_db = Url(url=url_status['expanded_url'])
                Session.add(url_in_db)

            tweet_url_in_db = Session.query(TweetUrl).filter(
                TweetUrl.url == url_status['url'],
                TweetUrl.indice_start == url_status['indices'][0],
                TweetUrl.tweet_id == status.id
            ).first()
            if not tweet_url_in_db:
                tweet_url_in_db = TweetUrl(
                    url=url_status['url'],
                    indice_start=url_status['indices'][0],
                    indice_end=url_status['indices'][1],
                    display_url=url_status.get('display_url', None),
                    expanded_url=url_status['expanded_url']
                )
                # tweet_url_in_db = update_tweet_url(tweet_url_in_db)
            urls.append(tweet_url_in_db)

        # Move all of this to status._json ???
        tweet_data = {
            'id': status.id,
            'created_at': status.created_at,
            'author_id': status.author.id,

            'hashtags': status.entities['hashtags'],
            'urls_jsonb': status.entities['urls'],
            'media': status.entities.get('media', None),
            'urls': urls,
            'user_mentions': status.entities['user_mentions'],

            'favorite_count': status.favorite_count,
            'is_quote_status': status.is_quote_status,
            'lang': status.lang,
            'possibly_sensitive': status.possibly_sensitive if hasattr(status, 'possibly_sensitive') else None,

            'retweeted': status.retweeted,
            'retweet_count': status.retweet_count,
            'is_retweet': True if status.text[:3] == "RT " else False,
            'original_tweet_id': status.retweeted_status.id if hasattr(status, 'retweeted_status') else None,

            'source': status.source,
            'text': status.text,
            'truncated': status.truncated
        }
        if hasattr(status, 'coordinates'):
            tweet_data['coordinates'] = status.coordinates
        if hasattr(status, 'metadata'):
            tweet_data['_metadata'] = status.metadata
        if hasattr(status, 'place'):
            if status.place:  # is not None
                tweet_data['place'] = status._json.get('place')
            else:
                tweet_data['place'] = status.place

        tweet_in_db = Session.query(Tweet).filter(Tweet.id == status.id).first()
        if tweet_in_db:
            try:
                print("Committing (update): {}".format(status.id))
                for col in ['favorite_count', 'retweeted', 'retweet_count']:
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

    except Exception as e:
        print(sys.stderr, 'Encountered Exception: ', e)
        Session.rollback()
        return


if __name__ == '__main__':
    # create our db
    print(Base.metadata.create_all(db))
