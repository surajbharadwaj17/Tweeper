
from backend.settings.credentials import *
import tweepy

class TweepyHandle:
    def __init__(self) -> None:
        self.auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
        self.auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        
    def get_api(self):
        return tweepy.API(self.auth)

        