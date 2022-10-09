import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from datetime import timedelta
import re
import nltk
import itertools
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

def createConnection(host, database, user, password):
    """
    Create a connection to the database
    """
    conn = psycopg2.connect(host=host,database=database,user=user,password=password)
    return conn


con = createConnection(host = '34.122.213.143', database = 'tweeper', user = 'postgres', password = 'aruba')
cursor = con.cursor()

print("####Connecting to database####")
hour = datetime.datetime.now() - timedelta(hours=24)
hour = hour.strftime("%Y-%m-%d %H:00:00")
sql_statement = f"""SELECT id,text, created_at, polarity, subjectivity, user_location 
                     FROM company_tweets 
                     WHERE created_at > '{hour}';"""

dats = pd.read_sql(sql_statement, con)
con.close()
print("####Successfully pulled data from database####")
keys = ['Amazon','Netflix','Google','Apple']

for key in keys:
    if key == 'Amazon':
        amazon_df = dats[dats['text'].str.contains(key)]
    elif key == 'Netflix':
        netflix_df = dats[dats['text'].str.contains(key)]
    elif key == 'Google':
        google_df = dats[dats['text'].str.contains(key)]
    elif key == 'Apple':
        apple_df = dats[dats['text'].str.contains(key)]

#print(amazon_df.info())

states = ['Alabama', 'AL', 'Alaska', 'AK', 'Arizona', 'AZ', 'Arkansas', 'AR', 'California', 'CA', 'Colorado', 'CO', 'Connecticut', 'CT', 'Delaware', 'DE', 'Florida', 'FL', 'Georgia', 'GA', 'Hawaii', 'HI', 'Idaho', 'ID', 'Illinois', 'IL', 'Indiana', 'IN', 'Iowa', 'IA', 'Kansas', 'KS', 'Kentucky', 'KY', 'Louisiana', 'LA', 'Maine', 'ME', 'Maryland', 'MD', 'Massachusetts', 'MA', 'Michigan', 'MI', 'Minnesota', 'MN', 'Mississippi', 'MS', 'Missouri', 'MO', 'Montana', 'MT', 'Nebraska', 'NE', 'Nevada', 'NV', 'New Hampshire', 'NH', 'New Jersey', 'NJ', 'New Mexico', 'NM', 'New York', 'NY', 'North Carolina', 'NC', 'North Dakota', 'ND', 'Ohio', 'OH', 'Oklahoma', 'OK', 'Oregon', 'OR', 'Pennsylvania', 'PA', 'Rhode Island', 'RI', 'South Carolina', 'SC', 'South Dakota', 'SD', 'Tennessee', 'TN', 'Texas', 'TX', 'Utah', 'UT', 'Vermont', 'VT', 'Virginia', 'VA', 'Washington', 'WA', 'West Virginia', 'WV', 'Wisconsin', 'WI', 'Wyoming', 'WY']
state_dict = dict(itertools.zip_longest(*[iter(states)] *2 , fillvalue=""))
inv_state_dict = dict((v,k) for k,v in state_dict.items())

##### Amazon #####

## Polarity ##
amazon_df['created_at'] = pd.to_datetime(amazon_df['created_at'])
amazon_result = amazon_df.groupby(
    [pd.Grouper(key = 'created_at', freq = '600s'), 'polarity']
).count().unstack(fill_value=0).stack().reset_index()

amazon_result['polarity'] = amazon_result['polarity'].apply(lambda x: -1 if x < 0 else (1 if x > 0 else 0))
amazon_result = amazon_result.rename(columns = 
{'id': 'Count of Amazon mentions', 'created_at': 'Time'})
amazon_result['Keyword'] = 'Amazon'
amazon_time_series = amazon_result["Time"][amazon_result['polarity']==0].reset_index(drop=True)

## FD ##
content = ''.join(amazon_df['text'])
content = re.sub(r"http\S+", "", content)
content = content.replace('RT ', ' ').replace('&amp;', 'and')
content = re.sub('[^A-Za-z0-9]+', ' ', content)
content = content.lower()

tokenized = word_tokenize(content)
stop_words = set(stopwords.words('english'))
filtered_sentence = [w for w in tokenized if not w in stop_words]

fdist = FreqDist(filtered_sentence)
fd_amazon = pd.DataFrame(fdist.most_common(10), columns = ['Word', 'Frequency'])
fd_amazon['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

## GEO ## 
in_us = []
loc = amazon_df[['user_location']]
amazon_df = amazon_df.fillna(' ')

for item in amazon_df['user_location']:
    check = False
    for state in states:
        if state in item:
            in_us.append(state_dict[state] if state in state_dict else state)
            check = True
            break
    if not check:
        in_us.append(None)

amazon_geo_dist = pd.DataFrame(in_us, columns = ['State']).reset_index()
amazon_geo_dist = amazon_geo_dist.groupby('State').count()
amazon_geo_dist = amazon_geo_dist.rename(columns = {'index': 'Count'})
amazon_geo_dist = amazon_geo_dist.sort_values(by = 'Count', ascending = False).reset_index()

amazon_geo_dist['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")


##### Netflix #####

## Polarity ##
netflix_df['created_at'] = pd.to_datetime(netflix_df['created_at'])
netflix_result = netflix_df.groupby(
    [pd.Grouper(key = 'created_at', freq = '600s'), 'polarity']
).count().unstack(fill_value=0).stack().reset_index()

netflix_result['polarity'] = netflix_result['polarity'].apply(lambda x: -1 if x < 0 else (1 if x > 0 else 0))
netflix_result = netflix_result.rename(columns = 
{'id': 'Count of Netflix mentions', 'created_at': 'Time'})
netflix_result['Keyword'] = 'Netflix'
netflix_time_series = netflix_result["Time"][netflix_result['polarity']==0].reset_index(drop=True)

## FD ##
content = ''.join(netflix_df['text'])
content = re.sub(r"http\S+", "", content)
content = content.replace('RT ', ' ').replace('&amp;', 'and')
content = re.sub('[^A-Za-z0-9]+', ' ', content)
content = content.lower()

tokenized = word_tokenize(content)
stop_words = set(stopwords.words('english'))
filtered_sentence = [w for w in tokenized if not w in stop_words]

fdist = FreqDist(filtered_sentence)
fd_netflix = pd.DataFrame(fdist.most_common(10), columns = ['Word', 'Frequency'])
fd_netflix['date'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

## GEO ##
in_us = []
loc = netflix_df[['user_location']]
netflix_df = netflix_df.fillna(' ')

for item in netflix_df['user_location']:
    check = False
    for state in states:
        if state in item:
            in_us.append(state_dict[state] if state in state_dict else state)
            check = True
            break
    if not check:
        in_us.append(None)

netflix_geo_dist = pd.DataFrame(in_us, columns = ['State']).reset_index()
netflix_geo_dist = netflix_geo_dist.groupby('State').count()
netflix_geo_dist = netflix_geo_dist.rename(columns = {'index': 'Count'})
netflix_geo_dist = netflix_geo_dist.sort_values(by = 'Count', ascending = False).reset_index()

netflix_geo_dist['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

#### Google #####
## Polarity ##
google_df['created_at'] = pd.to_datetime(google_df['created_at'])
google_result = google_df.groupby(
    [pd.Grouper(key = 'created_at', freq = '600s'), 'polarity']
).count().unstack(fill_value=0).stack().reset_index()

google_result['polarity'] = google_result['polarity'].apply(lambda x: -1 if x < 0 else (1 if x > 0 else 0))
google_result = google_result.rename(columns =
{'id': 'Count of Google mentions', 'created_at': 'Time'})
google_result['Keyword'] = 'Google'
google_time_series = google_result["Time"][google_result['polarity']==0].reset_index(drop=True)

## FD ##
content = ''.join(google_df['text'])
content = re.sub(r"http\S+", "", content)
content = content.replace('RT ', ' ').replace('&amp;', 'and')
content = re.sub('[^A-Za-z0-9]+', ' ', content)
content = content.lower()

tokenized = word_tokenize(content)
stop_words = set(stopwords.words('english'))
filtered_sentence = [w for w in tokenized if not w in stop_words]

fdist = FreqDist(filtered_sentence)
fd_google = pd.DataFrame(fdist.most_common(10), columns = ['Word', 'Frequency'])
fd_google['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

## GEO ##
in_us = []
loc = google_df[['user_location']]
google_df = google_df.fillna(' ')

for item in google_df['user_location']:
    check = False
    for state in states:
        if state in item:
            in_us.append(state_dict[state] if state in state_dict else state)
            check = True
            break
    if not check:
        in_us.append(None)
google_geo_dist = pd.DataFrame(in_us, columns = ['State']).reset_index()
google_geo_dist = google_geo_dist.groupby('State').count()
google_geo_dist = google_geo_dist.rename(columns = {'index': 'Count'})
google_geo_dist = google_geo_dist.sort_values(by = 'Count', ascending = False).reset_index()

google_geo_dist['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

#### Apple #####
## Polarity ##
apple_df['created_at'] = pd.to_datetime(apple_df['created_at'])
apple_result = apple_df.groupby(
    [pd.Grouper(key = 'created_at', freq = '600s'), 'polarity']
).count().unstack(fill_value=0).stack().reset_index()

apple_df['polarity'] = apple_df['polarity'].apply(lambda x: -1 if x < 0 else (1 if x > 0 else 0))
apple_result = apple_result.rename(columns =
{'id': 'Count of Apple mentions', 'created_at': 'Time'})
apple_result['Keyword'] = 'Apple'
apple_time_series = apple_result["Time"][apple_result['polarity']==0].reset_index(drop=True)

## FD ##
content = ''.join(apple_df['text'])
content = re.sub(r"http\S+", "", content)
content = content.replace('RT ', ' ').replace('&amp;', 'and')
content = re.sub('[^A-Za-z0-9]+', ' ', content)
content = content.lower()

tokenized = word_tokenize(content)
stop_words = set(stopwords.words('english'))
filtered_sentence = [w for w in tokenized if not w in stop_words]

fdist = FreqDist(filtered_sentence)
fd_apple = pd.DataFrame(fdist.most_common(10), columns = ['Word', 'Frequency'])

fd_apple['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")

## GEO ##
in_us = []
loc = apple_df[['user_location']]
apple_df = apple_df.fillna(' ')

for item in apple_df['user_location']:
    check = False
    for state in states:
        if state in item:
            in_us.append(state_dict[state] if state in state_dict else state)
            check = True
            break
    if not check:
        in_us.append(None)
apple_geo_dist = pd.DataFrame(in_us, columns = ['State']).reset_index()
apple_geo_dist = apple_geo_dist.groupby('State').count()
apple_geo_dist = apple_geo_dist.rename(columns = {'index': 'Count'})
apple_geo_dist = apple_geo_dist.sort_values(by = 'Count', ascending = False).reset_index()

apple_geo_dist['date_hour'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")


##### INSERT INTO DATABASE #####
def insert_data_sentiment(result, table_name):
    inserts = []

    for item in result.itertuples():
        insert = (item[0],item[1], item[2], item[3], item[4], item[5], item[6], item[7])
        inserts.append(insert)

    sql_statement = f"""
        INSERT INTO {table_name} (id, time, polarity, mentions, text, subjectivity, user_location, keyword)
        VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        conn = createConnection(host = 'ec2-3-211-228-251.compute-1.amazonaws.com', database = 'd3dll6bk435ijj', user = 'iiigxtlbnjbjat', password = '6fd22642e5a345a55e27b13eacddd8e7a40b3130ad5c4fc8b9bb002a9c98927c')
        cursor = conn.cursor()  
        cursor.executemany(sql_statement, inserts)
        conn.commit()
        print(f"####Successfully inserted {table_name} data into database####")
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()

def insert_data_fd(result, table_name):
    inserts = []
    print(result.info())
    for item in result.itertuples():
        insert = (item[1], item[2], item[3])
        inserts.append(insert)

    sql_statement = f"""
        INSERT INTO {table_name} (word, frequency, date_hour)
    VALUES (%s, %s, %s)"""
    try:
        conn = createConnection(host = 'ec2-3-211-228-251.compute-1.amazonaws.com', database = 'd3dll6bk435ijj', user = 'iiigxtlbnjbjat', password = '6fd22642e5a345a55e27b13eacddd8e7a40b3130ad5c4fc8b9bb002a9c98927c')
        cursor = conn.cursor()  
        cursor.executemany(sql_statement, inserts)
        conn.commit()
        print(f"####Successfully inserted {table_name} data into database####")
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()

def insert_data_geo(result, table_name):
    inserts = []
    for item in result.itertuples():
        insert = (item[3], item[2], item[1])
        inserts.append(insert)

    sql_statement = f"""
        INSERT INTO {table_name} (date_hour, count, state)
    VALUES (%s, %s, %s)"""
    try:
        conn = createConnection(host = 'ec2-3-211-228-251.compute-1.amazonaws.com', database = 'd3dll6bk435ijj', user = 'iiigxtlbnjbjat', password = '6fd22642e5a345a55e27b13eacddd8e7a40b3130ad5c4fc8b9bb002a9c98927c')
        cursor = conn.cursor()  
        cursor.executemany(sql_statement, inserts)
        conn.commit()
        print(f"####Successfully inserted {table_name} data into database####")
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()

company_table = {
    'amazon': ['amazon_tweets_sentiment_v2', 'amazon_tweets_fd','amazon_tweets_geo'],
    'apple': ['apple_tweets_sentiment_v2', 'apple_tweets_fd','apple_tweets_geo'],
    'google': ['google_tweets_sentiment_v2', 'google_tweets_fd','google_tweets_geo'],
    'netflix': ['netflix_tweets_sentiment_v2', 'netflix_tweets_fd','netflix_tweets_geo']
}



for key in company_table:
    insert_data_sentiment(eval(key + '_result'), company_table[key][0])

    insert_data_fd(eval('fd_'+ key), company_table[key][1])
    
    insert_data_geo(eval(key + '_geo_dist'), company_table[key][2])
