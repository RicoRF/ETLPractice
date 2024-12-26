import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_x_etl(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, BEARER_TOKEN):

    # Setting up client object
    client = tweepy.Client(
        bearer_token=BEARER_TOKEN,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET
    )

    get_musk_tweet = client.get_users_tweets(id="44196397", max_results=5)

    print(get_musk_tweet)

    tweets_list = []
    for tweet in get_musk_tweet:
        refined_tweet = {"id": tweet.id,
                        "text": tweet.text}
        tweets_list.append(refined_tweet)

    # Create data frame
    df = pd.DataFrame(tweets_list)
    df.to_csv("elon_musk_tweets.csv")