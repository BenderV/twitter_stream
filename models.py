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
import os

# set up our local DB to store the tweets
database = os.environ['DATABASE_NAME']
user = os.environ['DATABASE_LOGIN']
host = os.environ['DATABASE_HOST']
password = os.environ['DATABASE_PASSWORD']

db = create_engine('postgresql://' + user + ':' + password + '@' + host + '/' + database)
Base = declarative_base(bind=db)
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
    # add? place
    possibly_sensitive = Column(Boolean)

    retweeted = Column(Boolean)
    retweet_count = Column(Integer)
    is_retweet = Column(Boolean)
    original_tweet_id = Column(BigInteger)

    source = Column(String)
    text = Column(String)
    truncated = Column(Boolean)


class TweetUrl(AuditMixin, Base):
    # TODO: add link table...
    __tablename__ = 'tweet_urls'

    id = Column(BigInteger, unique=True, autoincrement=True)
    tweet_id = Column(BigInteger, ForeignKey('tweets.id'), primary_key=True)
    tweet = relationship("Tweet", back_populates="urls")

    url = Column(String, primary_key=True)
    indice_start = Column(Integer, primary_key=True)
    indice_end = Column(Integer)
    display_url = Column(String)
    expanded_url = Column(String)

    real_url = Column(String)
    real_url_netloc = Column(String)
    real_url_path = Column(String)
    real_url_arxiv_id = Column(String)


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


if __name__ == '__main__':
    # create our db
    print(Base.metadata.create_all(db))
