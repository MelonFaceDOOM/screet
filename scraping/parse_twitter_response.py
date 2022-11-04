from collections import deque
import json
from datetime import datetime
from db.models import Tweet, TweetError, TwitterUser, TwitterLink, ParsedResponseData


def read_twitter_response(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        twitter_response_data = f.read()
        twitter_response_data = twitter_response_data.replace('\\u0000', '')
        twitter_response_data = twitter_response_data.split('\n')
    return twitter_response_data


def parse_twitter_response(json_text):
    try:
        json_data = json.loads(json_text)
    except:
        return None
    tweets = deque()
    links = deque()
    users = deque()
    errors_with_id = deque()
    errors_without_id = deque()
    if 'data' in json_data:
        tweets, links = parse_main_json(json_data['data'])
    if 'errors' in json_data:
        errors_with_id, errors_without_id = parse_error_json(json_data['errors'])
    if 'includes' in json_data:
        if 'users' in json_data['includes']:
            users = parse_users_json(json_data['includes']['users'])
        # if 'location' in json_data['includes']:
        #     locations = self.parse_location_json(json_data['includes']['location'])
    parsed_response_data = ParsedResponseData(tweets=tweets, users=users, errors_with_id=errors_with_id,
                                              links=links, errors_without_id=errors_without_id)
    return parsed_response_data


def parse_main_json(main_json):
    tweets = deque()
    links = deque()
    for tweet_json in main_json:
        created_at = datetime.strptime(tweet_json['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        tweet = Tweet(id=int(tweet_json['id']),
                      user_id=int(tweet_json['author_id']),
                      conversation_id=int(tweet_json['conversation_id']),
                      created_at=created_at,
                      tweet_text=tweet_json['text'])
        if 'public_metrics' in tweet_json:
            tweet.like_count = tweet_json['public_metrics']['like_count']
            tweet.retweet_count = tweet_json['public_metrics']['retweet_count']
            tweet.quote_count = tweet_json['public_metrics']['quote_count']
            tweet.reply_count = tweet_json['public_metrics']['reply_count']
        tweets.append(tweet)
        if 'entities' in tweet_json:
            if 'urls' in tweet_json['entities']:
                for url in tweet_json['entities']['urls']:
                    link = TwitterLink(tweet_id=tweet_json['id'],
                                       tco_url=url['url'],
                                       full_url=url['expanded_url'])
                    links.append(link)
    return tweets, links


def parse_users_json(users_json):
    users = deque()
    for user_json in users_json:
        created_at = datetime.strptime(user_json['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        user = TwitterUser(
            id=int(user_json['id']),
            created_at=created_at,
            followers_count=int(user_json['public_metrics']['followers_count']),
            following_count=int(user_json['public_metrics']['following_count']),
            tweet_count=int(user_json['public_metrics']['tweet_count']),
            verified=user_json['verified']
        )
        if 'location' in user_json:
            user.location = user_json['location']
        users.append(user)
    return users


def parse_error_json(errors_json):
    errors_with_id = deque()
    errors_without_id = deque()
    for error_json in errors_json:
        if 'title' in error_json:
            error = TweetError(title=error_json['title'].lower())
        else:
            error = TweetError(title="invalid request")
        if 'detail' in error_json:
            error.detail = error_json['detail']
        if 'type' in error_json:
            error.error_type = error_json['type']
        if 'parameters' in error_json:
            if 'message' in error_json['parameters']:
                error.message = error_json['parameters']['message']
        if 'resource_id' in error_json:
            error.tweet_id = int(error_json['resource_id'])
        if error.tweet_id > 0:
            errors_with_id.append(error)
        else:
            errors_without_id.append(error)
    return errors_with_id, errors_without_id


def parse_twitter_responses(twitter_responses):
    # TODO: do something with json_errors_from_file
    json_errors_from_file = []
    parsed_response_data_from_file = ParsedResponseData()
    for json_text in twitter_responses:
        parsed_response_data = parse_twitter_response(json_text=json_text)
        if parsed_response_data:
            parsed_response_data_from_file.concatenate(parsed_response_data)
        else:
            json_errors_from_file.append(json_text)
    return parsed_response_data_from_file


def record_json_errors(json_errors):
    json_errors = "\n".join(json_errors)
    with open('json_errors.txt', 'w', encoding='utf-8') as f:
        f.write(json_errors)


def save_locations(parsed_response_data):
    locations = []
    for user in parsed_response_data.users:
        if user.location:
            locations.append(user.location.strip())
    locations = list(set(locations))
    locations.sort()
    locations = "\n".join(locations)
    with open('locations.txt', 'w', encoding='utf-8') as f:
        f.write(locations)
