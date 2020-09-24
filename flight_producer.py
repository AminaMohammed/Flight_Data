from kafka import KafkaProducer
from time import sleep
import json

# it takes JSON serializer by default
producer = KafkaProducer(bootstrap_servers=['localhost:9096'],
                         api_version=(0,10,1))

f = open('flightdata.json','r')
json_data = f.read()
response_list = json.loads(json_data)

for msg in response_list:
    producer.send('flight-topic',json.dumps(msg).encode('utf-8'))
    sleep(5)


