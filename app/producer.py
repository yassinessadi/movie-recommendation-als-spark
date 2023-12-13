from confluent_kafka import Producer
import requests
import json
import logging

#----+----+----+----+----+#
#       topic name        #
#----+----+----+----+----+#
topic = "moviesrecommendationallinone"

#----+----+----+----+----+#
#       kafka broker      #
#----+----+----+----+----+#
kafka_config = {
    "bootstrap.servers": "localhost:9092", 
}


#----+----+----+----+----+#
#     Configure logging   #
#----+----+----+----+----+#
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#----+----+----+----+----+#
#    init kafk producer   #
#----+----+----+----+----+#
producer = Producer(kafka_config)


counter = 20500
while True:
    base_url = f"http://127.0.0.1:5000/api/movie"
    params = {
        "id" : counter
    }
    response = requests.get(base_url,params=params)
    if response.status_code == 200:
        data = json.dumps(response.json())
        producer.produce(topic, key="key", value=data)
        producer.flush()
        logger.info("Produced message: %s", data)
        counter += 1
    else:
        logger.error("Failed to fetch movie data. HTTP status code: %d", response.status_code)