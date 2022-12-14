import datetime
import time
import requests
import os
import json
from collections import deque
from requests.exceptions import ConnectionError


class TwitterAPIRequestor:
    def __init__(self, bearer_token, max_retries=5):
        self.headers = {"Authorization": "Bearer " + bearer_token}
        self.max_retries = max_retries
        self.rate_limit_reset_time = 0
        self.rate_limit_amount_remaining = 0
        self.set_default_rate_limits()
        
    def set_default_rate_limits(self):
        epoch = datetime.datetime.utcfromtimestamp(0)
        self.rate_limit_reset_time = (datetime.datetime.utcnow() - epoch).total_seconds() + 900  # Will be overwritten by the first response headers
        self.rate_limit_amount_remaining = 1  # this will be overwritten on the first request
        
    def update_rate_remaining_and_reset_time(self, response):
        # TODO: what if headers not present, i.e. if response is 404
        self.rate_limit_reset_time = int(response.headers['x-rate-limit-reset'])
        self.rate_limit_amount_remaining = int(response.headers['x-rate-limit-remaining'])
        
    def make_request(self, url):
        if self.rate_limit_amount_remaining == 0:
            self.sleep_until_reset_time()
            self.set_default_rate_limits()
        response = None
        retries = 0
        while not response and retries < self.max_retries:
            try:
                response = requests.get(url, headers=self.headers)
            except (ConnectionError, TimeoutError):
                response = None
            if response:
                if self.response_contains_too_many_requests_error(response):
                    response = None
                    retries += 1
                    print(f"too many requests error.")
                    self.sleep_until_reset_time()
            else:
                retries += 1
                print(f"retry {retries} failed.")
                time.sleep(10)
        if retries == self.max_retries:
            raise RuntimeError(f'No response from twitter after {self.max_retries} retries.')
        self.update_rate_remaining_and_reset_time(response)
        return response
    
    def response_contains_too_many_requests_error(self, response):
        too_many_requests_error = '{"title":"Too Many Requests","detail":"Too Many Requests",' \
                                  '"type":"about:blank","status":429}'
        if response.text == too_many_requests_error:
            return True
        return False
        
    def sleep_until_reset_time(self):
        time_to_sleep = seconds_until_unix_time(self.rate_limit_reset_time) + 2
        if time_to_sleep < 0:
            time_to_sleep = 2
        print(f"sleeping for {time_to_sleep}")
        time.sleep(time_to_sleep)
        

class TwitterIdHydrator(TwitterAPIRequestor):
    def __init__(self, tweet_ids, bearer_token, ids_per_request=100):
        super().__init__(bearer_token=bearer_token)
        """tweet_ids is a deque
        ids_per_request is int 1-100"""
        self.tweet_ids = tweet_ids
        self.ids_per_request = ids_per_request
        
    def hydrate_ids(self):
        while self.tweet_ids:
            id_chunk = self.pop_id_chunk()
            url = build_api_url_with_ids(id_chunk)
            response = self.make_request(url)
            yield response.text
            
    def pop_id_chunk(self):
        id_chunk = []
        for i in range(self.ids_per_request):
            try:
                id_chunk.append(self.tweet_ids.popleft())
            except IndexError:
                break
        return id_chunk
    
                
class MultiFileHydrator(TwitterIdHydrator):
    def __init__(self, files_to_hydrate, output_folder, bearer_token, ids_per_request=100):
        super().__init__(tweet_ids=[], bearer_token=bearer_token, ids_per_request=ids_per_request)
        self.output_folder = output_folder
        self.files_to_hydrate = files_to_hydrate
        self.current_sample_progress_file_path = ""
        self.current_sample_output_file_path = ""
        self.current_sample_hydrated_id_count = 0
        
    def hydrate_files_and_save_output(self):
        for file_path in self.files_to_hydrate:
            directory, file_name = os.path.split(file_path)
            self.update_progress_file_path(file_name)
            self.update_output_file_path(file_name)
            self.current_sample_hydrated_id_count = self.read_hydrated_id_count_from_progress_file()
            self.tweet_ids = self.get_unhydrated_tweet_ids_from_file(file_path)
            for raw_twitter_response_json in self.hydrate_ids():
                raw_twitter_response_json += "\n"
                self.save_tweets_raw_json(raw_twitter_response_json)
                self.update_and_record_progress()
            self.delete_progress_file()
        
    def get_progress_file_name(self, tweet_id_file_name):
        return tweet_id_file_name.split('.')[0] + "_progress.txt"  # i.e. "panacea_sample_0.txt" -> panacea_sample_0_progress.txt
        
    def get_output_file_name(self, tweet_id_file_name):
        return tweet_id_file_name.split('.')[0] + "_hydrated.txt"  # i.e. "panacea_sample_0.txt" -> panacea_sample_0_hydrated.txt
    
    def update_progress_file_path(self, tweet_id_file_name):
        progress_file_name = self.get_progress_file_name(tweet_id_file_name)
        self.current_sample_progress_file_path = os.path.join(self.output_folder, progress_file_name)
        
    def update_output_file_path(self, tweet_id_file_name):
        output_file_name = self.get_output_file_name(tweet_id_file_name)
        self.current_sample_output_file_path = os.path.join(self.output_folder, output_file_name)
        
    def read_hydrated_id_count_from_progress_file(self):
        if not os.path.isfile(self.current_sample_progress_file_path):
            return 0
        with open(self.current_sample_progress_file_path, 'r') as f:
            hydrated_id_count = int(f.read().strip())
        print(hydrated_id_count)  # TODO: delete
        return hydrated_id_count
    
    def get_unhydrated_tweet_ids_from_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            tweet_ids = f.read()
        tweet_ids = tweet_ids.split("\n")
        if self.current_sample_hydrated_id_count > 0:
            tweet_ids = tweet_ids[self.current_sample_hydrated_id_count:]
        return deque(tweet_ids)
        
    def update_and_record_progress(self):
        self.current_sample_hydrated_id_count += self.ids_per_request
        with open(self.current_sample_progress_file_path, 'w+') as f:
            f.write(str(self.current_sample_hydrated_id_count))

    def delete_progress_file(self):
        os.remove(self.current_sample_progress_file_path)
        
    def save_tweets_raw_json(self, raw_json):
        with open(self.current_sample_output_file_path, 'a+', encoding='utf-8') as f:
            f.write(raw_json)
                

class TwitterSearcher(TwitterAPIRequestor):
    def __init__(self, search_terms, bearer_token, next_token=None, until_id=None, since_id=None):
        super().__init__(bearer_token=bearer_token)
        self.search_terms = search_terms
        self.next_token = next_token
        self.until_id = until_id
        self.since_id = since_id

    def search_loop(self):
        while True:
            extra_url_components = {'next_token': self.next_token,
                                'until_id': self.until_id,
                                'since_id': self.since_id}
            url = build_api_url_with_search_terms(search_terms=self.search_terms,
                                   extra_url_components=extra_url_components)
            response = self.make_request(url)
            next_token, newest_id, oldest_id, result_count = extract_search_meta_data(response)
            self.next_token = next_token
            yield response
            if not self.next_token:
                break
                
                
class SearchManager(TwitterSearcher):
    def __init__(self, search_terms, output_file, bearer_token, total_tweet_limit=None):
        super().__init__(search_terms=search_terms, bearer_token=bearer_token)
        self.progress_file = ''
        self.output_file = output_file
        self.total_tweet_limit = total_tweet_limit  # None = No limit
        self.create_or_locate_progress_file()
        
    def create_or_locate_progress_file(self):
        output_directory, output_file_name = os.path.split(self.output_file)
        _ = output_file_name.split(".")
        _ = ".".join(_[:-1]) if len(_) > 1 else _[0]
        progress_file_name = _ + "_progress.txt"
        self.progress_file = os.path.join(output_directory, progress_file_name)
        if not os.path.isfile(self.progress_file):
            progress_dict = {'next_token': None, 'newest_id': None, 'oldest_id': None, 'result_count': 0}
            with open(self.progress_file, 'w') as f:
                json.dump(progress_dict, f)
                
    def read_progress_file(self):
        with open(self.progress_file, 'r') as f:
            progress_dict = json.load(f)
        return progress_dict
        
    def update_progress_dict(self, next_token, oldest_id, newest_id, result_count):
        progress_dict = self.read_progress_file()
        progress_dict['result_count'] += result_count
        if newest_id:
            if not progress_dict['newest_id'] or (int(newest_id) > int(progress_dict['newest_id'])):
                progress_dict['newest_id'] = newest_id
        if oldest_id:
            if not progress_dict['oldest_id'] or (int(oldest_id) < int(progress_dict['oldest_id'])):
                progress_dict['oldest_id'] = oldest_id
        if next_token:
            progress_dict['next_token'] = next_token
        return progress_dict
    
    def update_progress_file(self, progress_dict):
        with open(self.progress_file, 'w') as f:
            json.dump(progress_dict, f)
            
    def start_search_loop_and_save_results(self):
        for response in self.search_loop():
            raw_json = self.extract_raw_json_from_response(response)
            self.save_raw_json(raw_json)
            next_token, newest_id, oldest_id, result_count = extract_search_meta_data(response)
            progress_dict = self.update_progress_dict(next_token, oldest_id, newest_id, result_count)
            self.update_progress_file(progress_dict)
            if self.total_tweet_limit:
                if progress_dict['result_count'] >= self.total_tweet_limit:
                    break
        os.remove(self.progress_file)  # TODO test
            
    def do_single_search(self):
        for response in search_loop():
            return response
            
    def continue_search(self): 
        progress = self.read_progress_file()
        self.next_token = progress['next_token']
        response = self.do_single_search()
        if response_contains_invalid_token_error(response):
            # If next_token fails, use until_id with oldest_id instead
            self.next_token = None
            self.until_id = progress['oldest_id']
        self.start_search_loop_and_save_results()
        
    def start_new_search(self, since_id=None):
        # since_id is needed to prevent overlap if a previous search was done within the same week
        self.since_id = since_id
        self.next_token = None
        self.start_search_loop_and_save_results()
        
    def extract_raw_json_from_response(self, response):
        raw_json = response.text
        raw_json += "\n"
        return raw_json
        
    def save_raw_json(self, raw_json):
        with open(self.output_file, 'a+', encoding='utf-8') as f:
            f.write(raw_json)
            

def build_api_url_with_ids(ids):
    url_components = {
        'expansions': ['author_id', 'geo.place_id'],
        'tweet.fields': ['id', 'author_id', 'conversation_id', 'created_at', 'public_metrics', 'in_reply_to_user_id',
                         'geo', 'text', 'referenced_tweets', 'entities'],
        'user.fields': ['id', 'verified', 'created_at', 'location', 'public_metrics'],
        'place.fields': ['country']
    }
    ids_text = ",".join(ids)
    url = f"https://api.twitter.com/2/tweets?ids={ids_text}"
    for key in url_components:
        component_text = ",".join(url_components[key])
        url += f"&{key}={component_text}"
    return url
    
    
def build_api_url_with_search_terms(search_terms, max_results=100, extra_url_components=None):
    """extra_url_components may contain: next_token, until_id, since_id"""
    query_string = f"{' OR '.join(search_terms)} lang:en -is:retweet"
    url = f"https://api.twitter.com/2/tweets/search/recent?query={query_string}"
    url_components = {
        'tweet.fields': ['id', 'author_id', 'conversation_id', 'created_at', 'public_metrics', 'in_reply_to_user_id',
                         'geo', 'text', 'referenced_tweets', 'entities'],
        'expansions': ['author_id', 'geo.place_id'],
        'place.fields': ['country'],
        'user.fields': ['id', 'verified', 'created_at', 'location', 'public_metrics'],
        'max_results': [str(max_results)]
    }
    if extra_url_components:
        for key in extra_url_components:
            if extra_url_components[key]:
                url_components[key] = [extra_url_components[key]]
    for key in url_components:
        component_text = ",".join(url_components[key])
        url += f"&{key}={component_text}"
    return url
    

def seconds_until_unix_time(unix_timestamp):
    epoch = datetime.datetime.utcfromtimestamp(0)
    seconds_since_epoch = (datetime.datetime.utcnow() - epoch).total_seconds()
    return unix_timestamp - seconds_since_epoch
    
def extract_search_meta_data(response):
    meta = response.json()["meta"]
    fields = ['next_token', 'newest_id', 'oldest_id', 'result_count']
    extracted = []
    for field in fields:
        if field in meta:
            extracted.append(meta[field])
        else:
            extracted.append(None)
    return extracted
