from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import get_type_hints


@dataclass
class FlattenableDataClass:
    @classmethod
    def attrs(cls):
        return [attr for attr in get_type_hints(cls)]


@dataclass
class Tweet(FlattenableDataClass):
    id: int
    user_id: int
    conversation_id: int
    created_at: datetime
    tweet_text: str
    source: str = ''
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    quote_count: int = 0

    def __repr__(self):
        return f"Tweet <{self.id}>: {self.tweet_text}"


@dataclass
class TweetError(FlattenableDataClass):
    title: str
    detail: str = ""
    error_type: str = ""
    message: str = ""
    tweet_id: int = 0

    def __repr__(self):
        return f"TwitterError <{self.tweet_id}>: {self.title}"


@dataclass
class TwitterUser(FlattenableDataClass):
    id: int
    created_at: datetime
    followers_count: int
    following_count: int
    tweet_count: int
    verified: bool
    location: str = ""

    def __repr__(self):
        return f"TwitterUser <{self.id}>"


@dataclass
class TwitterLink(FlattenableDataClass):
    tweet_id: int
    tco_url: str
    full_url: str = ""

    def __repr__(self):
        return f"TwitterLink <{self.tweet_id}>: {self.tco_url}"


@dataclass
class ParsedResponseData:
    """holds lists of parsed data from many tweets"""
    tweets: deque = field(default_factory=lambda: deque())
    links: deque = field(default_factory=lambda: deque())
    users: deque = field(default_factory=lambda: deque())
    errors_with_id: deque = field(default_factory=lambda: deque())
    errors_without_id: deque = field(default_factory=lambda: deque())

    def concatenate(self, other):
        self.tweets += other.tweets
        self.links += other.links
        self.users += other.users
        self.errors_with_id += other.errors_with_id
        self.errors_without_id += other.errors_without_id

    def __repr__(self):
        return f"{len(self.tweets)} tweets\n" \
               f"{len(self.links)} links\n" \
               f"{len(self.users)} users\n" \
               f"{len(self.errors_with_id)} errors with id\n" \
               f"{len(self.errors_without_id)} errors without id\n"


@dataclass
class TwitterVaccineMention(FlattenableDataClass):
    tweet_id: int
    vaccine_mentioned: str

    def __repr__(self):
        return f"TwitterLink <{self.tweet_id}>: {self.vaccine_mentioned}"


@dataclass
class FactCheckerArticle(FlattenableDataClass):
    review_date: datetime
    claim: str
    rating: str
    article_link: str
    article_domain: str
    article_source_link: str
    article_html: str = ""
    true_source_link: str = ""

    def __repr__(self):
        return self.article_link
