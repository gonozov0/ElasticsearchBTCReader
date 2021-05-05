import requests, logging, time
from datetime import datetime
from elasticsearch import Elasticsearch


def connect_elasticsearch():
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


def create_index(es_object, index_name='btc-usdt'):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "price": {
                    "type": "double"
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    if es is not None and create_index(es, index_name="btcusdt"):
        while True:
            res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT").json()
            body ={
                "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "price": res['price']
            }
            print(body)
            es.index('btcusdt', body=body)
            time.sleep(60)
