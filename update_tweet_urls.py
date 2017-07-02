from models import Session, TweetUrl
import urllib.request as urlreq
from urllib.parse import urlparse


def shortlink_dest(url):
    try:
        link = urlreq.urlopen(url, timeout=5)
        return link.url
    except Exception as e:
        print(e)
        return url


not_short_link = set()
short_links = set(['aka.ms', 'drumup.io', 'dld.bz',
                   'goo.gl', 'buff.ly', 'htn.to', 'dx.doi.org',
                   'l.dds.ec', 'ift.tt', 'doi.org', 'nzzl.us',
                   'Academia.edu', 'dlvr.it', 'j.mp', 'up5.fr',
                   'lnkd.in', 'ln.is', 'tinyurl.com', 'fb.me', 'youtu.be'])


def update_tweet_url(row):
    global not_short_link
    global short_links

    o = urlparse(row.expanded_url)
    if o.netloc not in not_short_link:
        url = shortlink_dest(row.expanded_url)
        if urlparse(url).netloc == o.netloc:
            not_short_link |= {o.netloc}
        else:
            short_links |= {o.netloc}
            print('Short link:', o.netloc)
    else:
        url = row.expanded_url

    o = urlparse(url)
    setattr(row, 'real_url', url)
    setattr(row, 'real_url_netloc', o.netloc)
    setattr(row, 'real_url_path', o.path)
    return url


if __name__ == '__main__':
    for row in Session.query(TweetUrl).filter(TweetUrl.real_url == None).all():
        print(row.expanded_url)
        url = update_tweet_url(row)
        print('=> ', url)
    print(short_links)

    Session.commit()
