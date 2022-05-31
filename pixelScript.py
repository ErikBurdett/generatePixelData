import logging
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json

now_json = str(datetime.today())
print(now_json)


nurl_typeDict = {
    'value':{

        'advertiser' : 'advertisers_ids',
        'price' : 100,
        'impression' : 'impression_id',
        'campaign' : 'campaign_id',
        'publisher' : 'publisher_id',
        'clearing_price_miocros_usd': 100,
        'bid' : 100,
        'user' : 'insert_ips here',
        'creative' : 'creative_here',
        'timestamp' : str(now_json)

    },
    'timestamp': str(now_json)
}

nurl_impressionDict = {
    'value':{
        'impression' : 'impressions',
        'valid' : 'true or false',
        'win_time_usec' : str(now_json),
        'timestamp' : str(now_json)
    },
    'timestamp': str(now_json)
}

snowflake_getEventDict = {
    'value':{
        'impression' : 'impression',
        'event' : 'complete or lost',
    },
    'timestamp': str(now_json)
}



# these JSON objects represent impressions and attributions and will be used in POST requests in the SSP tha
nurl_type_nurl_json = json.dumps(nurl_typeDict)
nurl_type_impression_json = json.dumps(nurl_impressionDict)
snowflake_get_event_json = json.dumps(snowflake_getEventDict)

print(nurl_type_nurl_json)
print(nurl_type_impression_json)
print(snowflake_get_event_json)



# next create these dynamically according to a 52 week playbook
order_id = 1
order_amount = 10
form_id = 1

test_adv_uuids = [
    '08d79951-1908-4f0f-91aa-40eee5ac0e3f',
    '30816356-4be2-4def-9793-2913dc7dae82',
    '40a9420a-3bad-4778-930d-0e3da35355d1',
    '9c2c4477-21ea-4b90-b72b-db40c8ae9754',
    '6f0fa70b-4914-458d-8b02-0b6a4475773f',
    '9614bd9f-afa0-4d93-b709-beb38c99cd66',
    '0de19a72-2016-495e-bf63-4b2de6bc9069',
    '543af5d0-d1c8-48bb-ac0e-402a9a0e2a04',
    'f8633b4d-4d48-436a-a9d0-d900197df443',
    '39314184-9194-4f51-a988-788a8f4a5436',
    '4bb2f9f8-2718-45b0-bc7d-db9c641fd978',
    '36f3d731-f829-4d2f-a864-abdc22d37069',
    'edcfb4c9-0137-49bb-8388-8091849a8c88',
    '18f5930b-06c2-47fe-bafa-89b53504657a',
    '6d302df1-9c36-43d6-ad30-571e3a3edb11',
    'c51cb4f2-5369-4bbe-a154-af6066cfb34a',
    '1ec91dae-7c90-474f-ae4c-eba2069237a2',
    '7da45c63-a8df-4e11-a3f6-20ad9ba94f8d',
    '43669b13-14f9-43c8-a56d-8e2b1d365ada',
    'b72f1e5a-e62c-4885-83ed-7d8d58d2cfa7',
    'b1bb0be4-c690-4b81-817d-5378a17377c7',
    '8a479c0a-c72f-48f8-b511-11d8568274cb',
    '95296d50-c55f-4859-afa6-797c1ef617ae',
    '7f50b703-6dc1-442a-9659-6a0e30be0519',
    '20dda5fb-c668-4660-a5d9-515ebf54fcf5',
    'd85aa020-cec5-4d06-ba79-9a1a7ad2591f',
    '6f5a831e-285c-4783-b9f1-62b6a04ed9f2',
    '4d300765-852e-4aa6-a186-a001942b6333',
    '85f345f9-dec0-47ed-bc82-dddf15f3810f',
    '3cedf522-bf70-45a8-8e9e-184c1b4967dd',
    '17823f24-04b1-4737-8e2b-1397f0743c8a',
    'f61a8667-260c-4b07-bd61-5c449e4d9264',
    'afac07e9-b75e-4a72-a444-362fbc4c1a2f',
    '9d85a2aa-a8f2-42ab-8903-0e578fa5b8c9',
    '1b5c636d-bd79-4ff5-b19f-be98478ec248',
    '3cc79fdc-a379-4141-b496-c5b3984144d4',
    '50041208-153f-44cf-87af-2d779feb01a1',
    '0c765987-2e2a-4a77-baa1-9b97a1799947',
    '510647d1-8ff4-4fdf-a2b5-937cbd2c1a98',
    '9be671e5-4368-4dea-8eae-d2bb40583335',
    '680205b1-6ddb-4317-a7d6-5d36a8e52380',
    '0feaa1ed-8c37-4128-bcc9-9aceec6849bc',
    'f3ebdb88-7978-4525-9e30-c8e28092bf45',
    '31ac77e6-040e-4acd-9918-6ae8492da4d1',
    '58e66880-53b2-4240-b0fe-5a20ad9b1731',
    'c6d603c1-3c46-4f49-9838-a5b7cc07d43e',
    '2e41ab7a-db87-41bc-93e8-4e7521c4d734',
    'bbb31534-ab1d-4f2e-b974-4cdf6d16b7aa',
    '7dd371fa-0fb5-4469-8d05-8b7853f31b43',
    'd44d62f1-c9e9-4557-905e-98b509a703ab',
    '02e03563-3e52-4a43-a08b-a54d09f8a7c7',
    '0f27354e-0afd-4cd6-ba2c-9eac57a27d04',
    'ab914145-46e2-4c93-be4c-de36ad83689b',
    '6ab0c01b-35ba-4cd3-ad2d-9feb7fd7ec5c',    
]

# next: add a few custom kinds like form completion etc 
test_adv_url_bodies = [f'https://listen.site.com/{body_uuid}/pixel.png' for body_uuid in test_adv_uuids]
test_adv_url_attrs = [f'https://listen.site.com/{test_adv_uuid}/pixel.png?order={order_id}&value={order_amount}' for test_adv_uuid in test_adv_uuids]
test_adv_url_form = [f'https://listen.site.com/{test_adv_form_uuid}/pixel.png?type=form&order={form_id}' for test_adv_form_uuid in test_adv_uuids]
test_adv_url_impression_no_attr =  [f'https://listen.site.com/{test_adv_form_uuid}/pixel.png?type=UTM=impression_no_attr' for test_adv_form_uuid in test_adv_uuids]
# test_adv_url_form = [f'https://listen.audiohook.com/{test_adv_form_uuid}/pixel.png?type=form&order={form_id}' for test_adv_form_uuid in test_adv_uuids]

# these date time functions create an IP address that also functions as a timestamp 
now = datetime.today()
last_minute = now - timedelta(hours=0, minutes=1)
week_day = datetime.today().weekday()
ip_now = f"10.0.{now.hour}.{now.minute}"
ip_last_minute = f"10.0.{last_minute.hour}.{last_minute.minute}"
ip_day = f"10.{now.day}.{last_minute.hour}.{last_minute.minute}"




THREAD_POOL = 16

session = requests.Session()
session.mount(
    'https://',
    requests.adapters.HTTPAdapter(pool_maxsize=THREAD_POOL,
                                  max_retries=3,
                                  pool_block=True)
)

def get(urls):
    # these headers contain the ip address/time stamps for pixel events 
    headers = {'A-Forwarded-For': f'{ip_now},{ip_last_minute},{ip_day}'}
    response = session.get(urls, headers=headers)
    logging.info("request was completed in %s seconds [%s]", response.elapsed.total_seconds(), response.url)
    if response.status_code != 200:
        logging.error("request failed, error code %s [%s]", response.status_code, response.url)
    if 500 <= response.status_code < 600:
        # server is overloaded? give it a break
        time.sleep(5)
    return response

def download(urls):
    with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
        # wrap in a list() to wait for all requests to complete
        for response in list(executor.map(get, urls)):
            if response.status_code == 200:
                print(response.status_code)
            if response.status_code != 200:
                logging.error("request failed, error code %s [%s]", response.status_code, response.url)

                # print(response.content)

def main():
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    

    urls = test_adv_url_bodies + test_adv_url_attrs + test_adv_url_form + test_adv_url_impression_no_attr

    download(urls)

if __name__ == "__main__":
    main()

