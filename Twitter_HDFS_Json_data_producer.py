from kafka import KafkaProducer
from time import sleep
from hdfs import InsecureClient
import json

HDFS_HOSTNAME = "localhost"
HDFS_PORT = '50070'
HDFS_CONNECTION_str = 'http://' + HDFS_HOSTNAME + ':' + HDFS_PORT

hdfs_client = InsecureClient(HDFS_CONNECTION_str)
file_path = "/home/fieldemployee/opt/flume/tweets/FlumeData.1600661442565"

with hdfs_client.read(file_path) as read_hdfs:
    content = read_hdfs.read()

twitter_data_json = content.decode('utf-8')
twitter_data_json = twitter_data_json.split('\n')

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9094'],
                         api_version=(0,10,1))

def buildOutDict(twitter_data_dict):

    hash_tags = []

    try:
        created_at = twitter_data_dict['created_at']
        try:
            try:
                name = twitter_data_dict['retweeted_status']['extended_tweet']['entities']['user_mentions'][0]['name']
                screen_name = twitter_data_dict['retweeted_status']['extended_tweet']['entities']['user_mentions'][0]['screen_name']
            except Exception:
                name = "null"
                screen_name = "null"

            full_text = twitter_data_dict['retweeted_status']['extended_tweet']['full_text']
            retweet_cnt = twitter_data_dict['retweeted_status']['retweet_count']
            favorite_cnt = twitter_data_dict['retweeted_status']['favorite_count']
            [hash_tags.append(tag['text']) for tag in twitter_data_dict['retweeted_status']['extended_tweet']['entities']['hashtags']]
        except Exception:
            try:
                name = twitter_data_dict['extended_tweet']['entities']['user_mentions'][0]['name']
                screen_name = twitter_data_dict['extended_tweet']['entities']['user_mentions'][0]['screen_name']
            except Exception:
                name = "null"
                screen_name = "null"

            full_text = twitter_data_dict['extended_tweet']['full_text']
            retweet_cnt = twitter_data_dict['retweet_count']
            favorite_cnt = twitter_data_dict['favorite_count']
            [hash_tags.append(tag['text']) for tag in twitter_data_dict['extended_tweet']['entities']['hashtags']]

        out_dict = {
            'created_at': created_at,
            'name': name,
            'screen_name': screen_name,
            'full_text': full_text,
            'retweet_cnt': retweet_cnt,
            'favorite_cnt': favorite_cnt,
            'hash_tags': hash_tags
        }
    
    except Exception:
        out_dict = 'null'
    
    return out_dict

for msg in twitter_data_json:
    try:
        msg_dict = json.loads(msg)
        msg_dict_filt = buildOutDict(msg_dict)
        if msg_dict_filt != 'null':
            producer.send('twitter-topic',json.dumps(msg_dict_filt).encode('utf-8'))
            #producer.send('zoo-lion',msg.encode('utf-8'))
            sleep(5)
    except Exception:
        pass
